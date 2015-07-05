package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext

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

/**
 * CreateTableAndLoadData
 *
 */
class HBaseTestData extends HBaseIntegrationTestBase {
  val StringKeyTableName = "StringKeyTable" // table with the sole STRING key
  val TestTableName = "TestTable"
  val StringKeyHBaseTableName: String = s"Hb$StringKeyHBaseTableName"
  val TestHBaseTableName: String = s"Hb$TestTableName"
  val TestHbaseColFamilies = Seq("cf1", "cf2")

  val CsvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  val DefaultLoadFile = "testTable.txt"

  private val tpath = for (csvPath <- CsvPaths
                           if new java.io.File(csvPath).exists()
  ) yield {
    logInfo(s"Following path exists $csvPath")
    csvPath
  }
  private[hbase] val CsvPath = tpath(0)

  override protected def beforeAll() = {
    super.beforeAll()
    val testTableCreationSQL = s"""CREATE TABLE $TestTableName(strcol STRING, bytecol BYTE,
                               shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($TestHBaseTableName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
      .stripMargin
    val stringKeyTableCreationSQL = s"""CREATE TABLE $StringKeyTableName(strcol STRING,
                                    bytecol BYTE, shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE, PRIMARY KEY(strcol))
            MAPPED BY ($StringKeyHBaseTableName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol,
            doublecol=cf1.hdoublecol, intcol=cf2.hintcol])"""
      .stripMargin
    createTable(TestTableName, TestHBaseTableName, testTableCreationSQL)
    createTable(StringKeyTableName, StringKeyHBaseTableName, stringKeyTableCreationSQL)
    loadData(TestTableName, s"$CsvPath/$DefaultLoadFile")
    loadData(StringKeyTableName, s"$CsvPath/$DefaultLoadFile")
  }

  override protected def afterAll() = {
    super.afterAll()
    TestHbase.sql("DROP TABLE " + StringKeyTableName)
    TestHbase.sql("DROP TABLE " + TestTableName)
  }

  def createNativeHbaseTable(tableName: String, families: Seq[String]) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    val hdesc = new HTableDescriptor(TableName.valueOf(tableName))
    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
    try {
      hbaseAdmin.createTable(hdesc)
    } catch {
      case e: TableExistsException =>
        logError(s"Table already exists $tableName", e)
    }
  }

  def dropNativeHbaseTable(tableName: String) = {
    try {
      val hbaseAdmin = TestHbase.hbaseAdmin
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.deleteTable(tableName)
    } catch {
      case e: TableExistsException =>
        logError(s"Table already exists $tableName", e)
    }
  }

  def createTable(tableName: String, hbaseTable: String, creationSQL: String) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTable))) {
      createNativeHbaseTable(hbaseTable, TestHbaseColFamilies)
    }

    if (TestHbase.catalog.checkLogicalTableExist(tableName)) {
      val dropSql = s"DROP TABLE $tableName"
      runSql(dropSql)
    }

    try {
      logInfo(s"invoking $creationSQL ..")
      runSql(creationSQL)
    } catch {
      case e: TableExistsException =>
        logInfo("IF NOT EXISTS still not implemented so we get the following exception", e)
    }
  }

  def loadData(tableName: String, loadFile: String) = {
    // then load data into table
    val loadSql = s"LOAD PARALL DATA LOCAL INPATH '$loadFile' INTO TABLE $tableName"
    runSql(loadSql)
  }

  def s2b(s: String) = Bytes.toBytes(s)

  def run(sqlCtx: SQLContext, testName: String, sql: String, exparr: Seq[Seq[Any]]) = {
    val result1 = runSql(sql)
    assert(result1.length == exparr.length, s"$testName failed on size")
    verify(testName,
      sql,
      for (rx <- exparr.indices)
      yield result1(rx).toSeq, exparr
    )
  }
}
