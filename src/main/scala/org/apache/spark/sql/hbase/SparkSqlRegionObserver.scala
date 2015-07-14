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

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor._
import org.apache.hadoop.hbase.regionserver._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.hbase.util.DataTypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
 * HBaseCoprocessorSQLReaderRDD:
 */
class HBaseCoprocessorSQLReaderRDD(var relation: HBaseRelation,
                                   val codegenEnabled: Boolean,
                                   var finalOutput: Seq[Attribute],
                                   var otherFilters: Option[Expression],
                                   @transient sqlContext: SQLContext)
  extends RDD[Row](sqlContext.sparkContext, Nil) with Logging {

  @transient var scanner: RegionScanner = _

  private def createIterator(context: TaskContext): Iterator[Row] = {
    val otherFilter: (Row) => Boolean = {
      if (otherFilters.isDefined) {
        if (relation.deploySuccessfully.isDefined && relation.deploySuccessfully.get) {
          null
        } else {
          if (codegenEnabled) {
            GeneratePredicate.generate(otherFilters.get, finalOutput)
          } else {
            InterpretedPredicate.create(otherFilters.get, finalOutput)
          }
        }
      } else null
    }

    val projections = finalOutput.zipWithIndex
    var finished: Boolean = false
    var gotNext: Boolean = false
    val results: java.util.ArrayList[Cell] = new java.util.ArrayList[Cell]()
    val row = new GenericMutableRow(finalOutput.size)

    val iterator = new Iterator[Row] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            results.clear()
            scanner.nextRaw(results)
            finished = results.isEmpty
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
          relation.buildRowInCoprocessor(projections, results, row)
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

  override def getPartitions: Array[Partition] = {
    Array()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    scanner = split.asInstanceOf[HBasePartition].newScanner
    createIterator(context)
  }
}

abstract class BaseRegionScanner extends RegionScanner {

  override def isFilterDone = false

  override def next(result: java.util.List[Cell], limit: Int) = next(result)

  override def reseek(row: Array[Byte]) = throw new DoNotRetryIOException("Unsupported")

  override def getMvccReadPoint = Long.MaxValue

  override def nextRaw(result: java.util.List[Cell]) = next(result)

  override def nextRaw(result: java.util.List[Cell], limit: Int) = next(result, limit)
}

class SparkSqlRegionObserver extends BaseRegionObserver {
  lazy val logger = Logger.getLogger(getClass.getName)
  lazy val EmptyArray = Array[Byte]()

  override def postScannerOpen(e: ObserverContext[RegionCoprocessorEnvironment],
                               scan: Scan,
                               s: RegionScanner) = {
    val serializedPartitionIndex = scan.getAttribute(CoprocessorConstants.COINDEX)
    if (serializedPartitionIndex == null) {
      logger.debug("Work without coprocessor")
      super.postScannerOpen(e, scan, s)
    } else {
      logger.debug("Work with coprocessor")
      val partitionIndex: Int = Bytes.toInt(serializedPartitionIndex)
      val serializedOutputDataType = scan.getAttribute(CoprocessorConstants.COTYPE)
      val outputDataType: Seq[DataType] =
        HBaseSerializer.deserialize(serializedOutputDataType).asInstanceOf[Seq[DataType]]

      val serializedRDD = scan.getAttribute(CoprocessorConstants.COKEY)
      val subPlanRDD: RDD[Row] = HBaseSerializer.deserialize(serializedRDD).asInstanceOf[RDD[Row]]

      val taskParaInfo = scan.getAttribute(CoprocessorConstants.COTASK)
      val (stageId, partitionId, taskAttemptId, attemptNumber) =
        HBaseSerializer.deserialize(taskParaInfo).asInstanceOf[(Int, Int, Long, Int)]
      val taskContext = new TaskContextImpl(
        stageId, partitionId, taskAttemptId, attemptNumber, null, false, new TaskMetrics)

      val regionInfo = s.getRegionInfo
      val startKey = if (regionInfo.getStartKey.isEmpty) None else Some(regionInfo.getStartKey)
      val endKey = if (regionInfo.getEndKey.isEmpty) None else Some(regionInfo.getEndKey)

      val result = subPlanRDD.compute(
        new HBasePartition(partitionIndex, partitionIndex, startKey, endKey, newScanner = s),
        taskContext)

      new BaseRegionScanner() {
        override def getRegionInfo: HRegionInfo = regionInfo

        override def getMaxResultSize: Long = s.getMaxResultSize

        override def close(): Unit = s.close()

        override def next(results: java.util.List[Cell]): Boolean = {
          val hasMore: Boolean = result.hasNext
          if (hasMore) {
            val nextRow = result.next()
            val numOfCells = outputDataType.length
            for (i <- 0 until numOfCells) {
              val data = nextRow(i)
              val dataType = outputDataType(i)
              val dataOfBytes: HBaseRawType = {
                if (data == null) null else DataTypeUtils.dataToBytes(data, dataType)
              }
              results.add(new KeyValue(EmptyArray, EmptyArray, EmptyArray, dataOfBytes))
            }
          }
          hasMore
        }
      }
    }
  }
}
