/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hbase


import org.apache.hadoop.hbase.client.{ResultScanner, Result, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hbase.util.{BytesUtils, HBaseKVHelper, DataTypeUtils}
import org.apache.spark.sql.types.NativeType
import org.apache.spark.{InterruptibleIterator, Logging, Partition, TaskContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
 * HBaseSQLReaderRDD
 */
class HBaseSQLReaderRDD(
                         relation: HBaseRelation,
                         codegenEnabled: Boolean,
                         output: Seq[Attribute],
                         filterPred: Option[Expression],
                         coprocSubPlan: Option[SparkPlan],
                         @transient sqlContext: SQLContext)
  extends RDD[Row](sqlContext.sparkContext, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    RangeCriticalPoint.generatePrunedPartitions(relation, filterPred).toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  private def createIterator(context: TaskContext,
                     scanner: ResultScanner, otherFilters: Option[Expression]): Iterator[Row] = {
    val lBuffer = ListBuffer[HBaseRawType]()
    val aBuffer = ArrayBuffer[Byte]()

    var finalOutput = output.distinct
    if (otherFilters.isDefined) {
      finalOutput = finalOutput.union(otherFilters.get.references.toSeq)
    }
    val row = new GenericMutableRow(finalOutput.size)
    val projections = finalOutput.zipWithIndex

    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null

    val otherFilter: (Row) => Boolean = if (otherFilters.isDefined) {
      if (codegenEnabled) {
        GeneratePredicate(otherFilters.get, finalOutput)
      } else {
        InterpretedPredicate(otherFilters.get, finalOutput)
      }
    } else null

    val iterator = new Iterator[Row] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            result = scanner.next
            finished = result == null
            gotNext = true
          }
        }
        if (finished) {
          close()
        }
        !finished
      }

      override def next(): Row = {
        if (hasNext) {
          gotNext = false
          relation.buildRow(projections, result, lBuffer, aBuffer, row)
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
          relation.closeHTable()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }
    if (otherFilter == null) {
      new InterruptibleIterator(context, iterator)
    } else {
      new InterruptibleIterator(context, iterator.filter(otherFilter))
    }
  }

  /**
   * construct row key based on the critical point range information
   * @param cpr the critical point range
   * @param isStart the switch between start and end value
   * @return the encoded row key
   */
  private def constructRowKey(cpr: MDCriticalPointRange[_], isStart: Boolean): HBaseRawType = {
    val prefix = cpr.prefix
    val head: Seq[(HBaseRawType, NativeType)] = prefix.map {
      case (itemValue, itemType) =>
        (DataTypeUtils.dataToBytes(itemValue, itemType), itemType)
    }

    val key = if (isStart) cpr.lastRange.start.get else cpr.lastRange.end.get
    val keyType = cpr.lastRange.dt
    val tail: (HBaseRawType, NativeType) = {
      (DataTypeUtils.dataToBytes(key, keyType), keyType)
    }

    HBaseKVHelper.encodingRawKeyColumns(head :+ tail)
  }

  // For critical-point-based predicate pushdown
  // partial reduction for those partitions mapped to multiple critical point ranges,
  // as indicated by the keyPartialEvalIndex in the partition, where the original
  // filter predicate will be used
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition = split.asInstanceOf[HBasePartition]
    val predicates = partition.computePredicate(relation)
    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      RangeCriticalPoint.generateCriticalPointRanges(relation, predicates).
        flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    if (expandedCPRs.isEmpty) {
      val (filters, otherFilters, pushdownPreds) = relation.buildPushdownFilterList(predicates)
      val pushablePreds = if (pushdownPreds.isDefined) {
        ListBuffer[Expression](pushdownPreds.get)
      } else {
        ListBuffer[Expression]()
      }
      val scan = relation.buildScan(partition.start, partition.end, filters, otherFilters,
        pushablePreds, output)
      val scanner = relation.htable.getScanner(scan)
      createIterator(context, scanner, otherFilters)
    } else {
      // expandedCPRs is not empty
      val isPointRanges = expandedCPRs.forall(
        p => p.lastRange.isPoint && p.prefix.size == relation.keyColumns.size - 1)
      if (isPointRanges) {
        // all of the last ranges are point range, build a list of get
        val gets: java.util.List[Get] = new java.util.ArrayList[Get]()

        val distinctProjList = output.distinct
        val nonKeyColumns = relation.nonKeyColumns.filter {
          case nkc => distinctProjList.exists(nkc.sqlName == _.name)
        }

        var resultRows: Iterator[Row] = null

        for (range <- expandedCPRs) {
          val rowKey = constructRowKey(range, isStart = true)
          val get = new Get(rowKey)
          for (nonKeyColumn <- nonKeyColumns) {
            get.addColumn(Bytes.toBytes(nonKeyColumn.family), Bytes.toBytes(nonKeyColumn.qualifier))
          }

          gets.add(get)
          val results = relation.htable.get(gets)
          val predicate = range.lastRange.pred

          val lBuffer = ListBuffer[HBaseRawType]()
          val aBuffer = ArrayBuffer[Byte]()
          val row = new GenericMutableRow(output.size)
          val projections = output.zipWithIndex

          resultRows = if (predicate == null) {
            results.map(relation.buildRow(projections, _, lBuffer, aBuffer, row)).toIterator
          } else {
            val boundPredicate = BindReferences.bindReference(predicate, output)
            results.map(relation.buildRow(projections, _, lBuffer, aBuffer, row))
              .filter(boundPredicate.eval(_).asInstanceOf[Boolean]).toIterator
          }
        }
        resultRows
      }
      else {
        // isPointRanges is false
        // calculate the range start
        val startKey: Option[Any] = expandedCPRs(0).lastRange.start
        val start = if (startKey.isDefined) {
          var rowKey = constructRowKey(expandedCPRs(0), isStart = true)
          if (partition.start.isDefined && Bytes.compareTo(partition.start.get, rowKey) > 0) {
            rowKey = partition.start.get
          }
          Some(rowKey)
        } else {
          partition.start
        }

        // calculate the range end
        val size = expandedCPRs.size - 1
        val endKey: Option[Any] = expandedCPRs(size).lastRange.end
        val endInclusive: Boolean = expandedCPRs(size).lastRange.endInclusive
        val end = if (endKey.isDefined) {
          var finalKey: HBaseRawType = {
            val rowKey = constructRowKey(expandedCPRs(size), isStart = false)
            if (endInclusive) {
              val newKey = BytesUtils.addOne(rowKey)
              if (newKey == null) {
                partition.end.get
              } else {
                newKey
              }
            } else {
              rowKey
            }
          }

          if (partition.end.isDefined && Bytes.compareTo(finalKey, partition.end.get) > 0) {
            finalKey = partition.end.get
          }
          Some(finalKey)
        } else {
          partition.end
        }

        val (filters, otherFilters, preds) =
          relation.buildCPRFilterList(output, filterPred, expandedCPRs)
        val scan = relation.buildScan(start, end, filters, otherFilters, preds, output)
        val scanner = relation.htable.getScanner(scan)
        createIterator(context, scanner, otherFilters)
      }
    }
  }
}
