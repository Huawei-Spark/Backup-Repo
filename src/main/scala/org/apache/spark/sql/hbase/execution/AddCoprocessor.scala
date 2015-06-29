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

package org.apache.spark.sql.hbase.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase._

private[hbase] case class AddCoprocessor(sqlContext: SQLContext) extends Rule[SparkPlan] {
  private lazy val catalog = sqlContext.asInstanceOf[HBaseSQLContext].catalog

  private def coprocessorIsAvailable(relation: HBaseRelation): Boolean = {
    catalog.deploySuccessfully.get && catalog.hasCoprocessor(relation.hbaseTableName)
  }

  private def generateNewSubplan(origPlan: SparkPlan): SparkPlan = {
    // reduce project (network transfer) if projection list has duplicate
    val (output, distinctOutput) = (origPlan.output, origPlan.output.distinct)
    val needToReduce: Boolean = distinctOutput.size < output.size
    val subplan: SparkPlan = {
      if (needToReduce) Project(distinctOutput, origPlan) else origPlan
    }

    // If any current directory of region server is not accessible,
    // we could not use codegen, or else it will lead to crashing the HBase region server!!!
    // For details, please read the comment in CheckDirEndPointImpl.
    var oldScan: HBaseSQLTableScan = null
    lazy val codegenEnabled = catalog.pwdIsAccessible && oldScan.codegenEnabled
    val newSubplan = subplan.transformUp {
      case subplanScan: HBaseSQLTableScan =>
        oldScan = subplanScan
        val rdd = new HBaseCoprocessorSQLReaderRDD(
          null, codegenEnabled, oldScan.output, None, sqlContext)
        HBaseSQLTableScan(oldScan.relation, oldScan.output, rdd)
    }

    val oldRDD: HBaseSQLReaderRDD = oldScan.result.asInstanceOf[HBaseSQLReaderRDD]
    val newRDD = new HBasePostCoprocessorSQLReaderRDD(
      oldRDD.relation, codegenEnabled, oldRDD.output,
      oldRDD.filterPred, newSubplan, sqlContext)
    val newScan = new HBaseSQLTableScan(oldRDD.relation, subplan.output, newRDD)

    // add project spark plan if projection list has duplicate
    if (needToReduce) Project(output, newScan) else newScan
  }

  def apply(plan: SparkPlan): SparkPlan = {
    var createSubplan: Boolean = false
    var createSubplanLeft: Boolean = false
    var path: List[SparkPlan] = List[SparkPlan]()
    plan match {
      // If the plan is tableScan directly, we don't need to use coprocessor
      case HBaseSQLTableScan(_, _, _) => plan
      case Filter(_, child: HBaseSQLTableScan) => plan
      case _ =>
        val result = plan.transformUp {
          case scan: HBaseSQLTableScan if coprocessorIsAvailable(scan.relation) =>
            createSubplanLeft = createSubplan
            createSubplan = true
            scan

          // If subplan is needed then we need coprocessor plans for both children
          case binaryNode: BinaryNode if createSubplanLeft || createSubplan =>
            val leftPlan: SparkPlan = if (createSubplanLeft) {
              createSubplanLeft = false
              generateNewSubplan(binaryNode.left)
            } else binaryNode.left
            val rightPlan: SparkPlan = if (createSubplan) {
              createSubplan = false
              generateNewSubplan(binaryNode.right)
            } else binaryNode.right
            // In trait trees.BinaryNode, the order of its children is (left, right).
            // Thus, we use the same order here to compose the new children
            val newChildren = Seq(leftPlan, rightPlan)
            binaryNode.withNewChildren(newChildren)

          // Since the following two plans using shuffledRDD,
          // we could not pass them to the coprocessor.
          // Thus, their child are used as the subplan for coprocessor processing.
          case exchange: Exchange if createSubplan =>
            createSubplan = false
            val newPlan = generateNewSubplan(exchange.child)
            exchange.withNewChildren(Seq(newPlan))
          case limit: Limit if createSubplan =>
            createSubplan = false
            val newPlan = generateNewSubplan(limit.child)
            limit.withNewChildren(Seq(newPlan))
        }
        // Use coprocessor even without shuffling
        if (createSubplan) generateNewSubplan(result) else result
    }
  }
}
