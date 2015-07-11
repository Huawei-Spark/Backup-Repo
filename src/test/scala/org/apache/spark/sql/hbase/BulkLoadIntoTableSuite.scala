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

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.Exchange
import org.apache.spark.sql.hbase.execution._
import org.apache.spark.sql.hbase.util.{BytesUtils, HBaseKVHelper}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLConf}

class BulkLoadIntoTableSuite extends HBaseTestData {
  val sc: SparkContext = TestHbase.sparkContext
  val hbaseHome = {
    val loader = this.getClass.getClassLoader
    val url = loader.getResource("loadData.txt")
    val file = new File(url.getPath)
    val parent = file.getParentFile
    parent.getAbsolutePath
  }
  if (hbaseHome == null || hbaseHome.isEmpty)
    logError("Spark Home is not defined; may lead to unexpected error!")

  // Test if we can parse 'LOAD DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb1'
  test("bulk load parser test, local file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD PARALL DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb1"

    val plan: LogicalPlan = parser.parse(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[BulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[BulkLoadIntoTableCommand]
    assert(l.inputPath.equals(raw"./usr/file.txt"))
    assert(l.isLocal)
    assert(l.tableName.equals("tb1"))
  }

  // Test if we can parse 'LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1'
  test("bulkload parser test, load hdfs file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD PARALL DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1"

    val plan: LogicalPlan = parser.parse(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[BulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[BulkLoadIntoTableCommand]
    assert(l.inputPath.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb1"))
  }

  test("bulkload parser test, using delimiter") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD PARALL DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1 FIELDS TERMINATED BY '\\|' "

    val plan: LogicalPlan = parser.parse(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[BulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[BulkLoadIntoTableCommand]
    assert(l.inputPath.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb1"))
    assert(l.delimiter.get.equals(raw"\\|"))
  }

  test("load data into hbase") {

    val drop = "drop table testblk"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (testblkHTable, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    TestHbase.sql(drop)
  }

  test("load parall data into hbase") {

    val drop = "drop table testblk"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        println(e.getMessage)
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (testblkHTable, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    TestHbase.sql(drop)
  }

  test("load data with null column values into hbase") {

    val drop = "drop table testNullColumnBulkload"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testNullColumnBulkload(col1 STRING, col2 STRING, col3 STRING, col4 STRING, PRIMARY KEY(col1))
          MAPPED BY (testNullColumnBulkloadHTable, COLS=[col2=cf1.a, col3=cf1.b, col4=cf1.c])"""
        .stripMargin

    val sql2 =
      s"""select * from testNullColumnBulkload"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadNullableData.txt'"

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testNullColumnBulkload"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    val sqlResult = TestHbase.sql("select * from testNullColumnBulkload")
    val rows = sqlResult.collect()
    assert(rows.length == 4, s"load parall data with null column values into hbase")
    assert(rows(0)(1) == null, s"load parall data into hbase test failed to select empty-string col1 value")
    assert(rows(1)(2) == null, s"load parall data into hbase test failed to select empty-string col2 value")
    assert(rows(2)(3) == null, s"load parall data into hbase test failed to select null col3 value")
    checkAnswer(sqlResult,
      Row("row1", null, "8", "101") ::
        Row("row2", "2", null, "102") ::
        Row("row3", "3", "10", null) ::
        Row("row4", null, null, null) :: Nil)

    // cleanup
    TestHbase.sql(drop)
  }

  test("load parall data with null column values into hbase") {

    val drop = "drop table testNullColumnBulkload"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        println(e.getMessage)
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testNullColumnBulkload(col1 STRING, col2 STRING, col3 STRING, col4 STRING, PRIMARY KEY(col1))
          MAPPED BY (testNullColumnBulkloadHTable, COLS=[col2=cf1.a, col3=cf1.b, col4=cf1.c])"""
        .stripMargin

    val sql2 =
      s"""select * from testNullColumnBulkload"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadNullableData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testNullColumnBulkload"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    val sqlResult = TestHbase.sql("select * from testNullColumnBulkload")
    val rows = sqlResult.collect()
    assert(rows.length == 4, s"load parall data with null column values into hbase")
    assert(rows(0)(1) == null, s"load parall data into hbase test failed to select empty-string col1 value")
    assert(rows(1)(2) == null, s"load parall data into hbase test failed to select empty-string col2 value")
    assert(rows(2)(3) == null, s"load parall data into hbase test failed to select null col3 value")
    checkAnswer(sqlResult,
      Row("row1", null, "8", "101") ::
        Row("row2", "2", null, "102") ::
        Row("row3", "3", "10", null) ::
        Row("row4", null, null, null) :: Nil)

    // cleanup
    TestHbase.sql(drop)
  }

  test("load data on hbase table with more than 1 column family") {
    createNativeHbaseTable("multi_cf_table", Seq("cf1", "cf2"))
    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (multi_cf_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    TestHbase.sql("drop table testblk")
    dropNativeHbaseTable("multi_cf_table")

  }

  test("load parall data on hbase table with more than 1 column family") {
    createNativeHbaseTable("multi_cf_table", Seq("cf1", "cf2"))
    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (multi_cf_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    TestHbase.sql("drop table testblk")
    dropNativeHbaseTable("multi_cf_table")
  }

  test("bulk load for presplit table") {
    val splitKeys = Seq(4, 8, 12).map { x =>
      BytesUtils.create(IntegerType).toBytes(x)
    }
    TestHbase.catalog.createHBaseUserTable("presplit_table", Set("cf1", "cf2"), splitKeys.toArray)

    val sql1 =
      s"""CREATE TABLE testblk(col1 INT, col2 INT, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (presplit_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/splitLoadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    assert(TestHbase.sql("select * from testblk").collect().length == 16)

    // cleanup
    TestHbase.sql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  test("parall bulk load for presplit table") {
    val splitKeys = Seq(4, 8, 12).map { x =>
      BytesUtils.create(IntegerType).toBytes(x)
    }
    TestHbase.catalog.createHBaseUserTable("presplit_table", Set("cf1", "cf2"), splitKeys.toArray)

    val sql1 =
      s"""CREATE TABLE testblk(col1 INT, col2 INT, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (presplit_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/splitLoadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    assert(TestHbase.sql("select * from testblk").collect().length == 16)

    // cleanup
    TestHbase.sql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  test("group test for presplit table with coprocessor but without codegen") {
    aggregationTest()
  }

  test("group test for presplit table without coprocessor and codegen") {
    aggregationTest(withCoprocessor = false)
  }

  test("group test for presplit table with codegen and coprocessor") {
    val originalValue = TestHbase.conf.codegenEnabled
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, "true")
    aggregationTest()
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("group test for presplit table with codegen but without coprocessor") {
    val originalValue = TestHbase.conf.codegenEnabled
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, "true")
    aggregationTest(withCoprocessor = false)
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  def aggregationTest(withCoprocessor: Boolean = true) = {
    val types = Seq(IntegerType, StringType, IntegerType)

    def generateRowKey(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    val splitKeys: Array[HBaseRawType] = Array(
      generateRowKey(Array(1024, UTF8String("0b"), 0), 3),
      generateRowKey(Array(2048, UTF8String("cc"), 1024), 4),
      generateRowKey(Array(4096, UTF8String("0a"), 0), 4),
      generateRowKey(Array(4096, UTF8String("0b"), 1024), 7),
      generateRowKey(Array(4096, UTF8String("cc"), 0), 7),
      generateRowKey(Array(4096, UTF8String("cc"), 1000))
    )
    TestHbase.catalog.createHBaseUserTable("presplit_table", Set("cf"), splitKeys, withCoprocessor)

    val sql1 =
      s"""CREATE TABLE testblk(col1 INT, col2 STRING, col3 INT, col4 STRING,
          PRIMARY KEY(col1, col2, col3))
          MAPPED BY (presplit_table, COLS=[col4=cf.a])"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/splitLoadData1.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    var sql = "select col1,col3 from testblk where col1 < 4096 group by col3,col1"
    checkResult(TestHbase.sql(sql), containExchange = true, 3)

    sql = "select col1,col2,col3 from testblk group by col1,col2,col3"
    checkResult(TestHbase.sql(sql), containExchange = false, 7)

    sql = "select col1,col2,col3,count(*) from testblk group by col1,col2,col3,col1+col3"
    checkResult(TestHbase.sql(sql), containExchange = false, 7) //Sprecial case

    sql = "select count(*) from testblk group by col1+col3"
    checkResult(TestHbase.sql(sql), containExchange = true, 5)

    sql = "select col1,col2 from testblk where col1 < 4096 group by col1,col2"
    checkResult(TestHbase.sql(sql), containExchange = false, 3)

    sql = "select col1,col2 from testblk where (col1 = 4096 and col2 < 'cc') group by col1,col2"
    checkResult(TestHbase.sql(sql), containExchange = true, 2)

    sql = "select col1 from testblk where col1 < 4096 group by col1"
    checkResult(TestHbase.sql(sql), containExchange = false, 3)

    val result = runSql("select avg(col3) from testblk where col1 < 4096 group by col1")
    assert(result.length == 3)
    val exparr = Array(Array(1024.0), Array(0.0), Array(1024.0))

    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    // cleanup
    TestHbase.sql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  test("UNION TEST") {
    unionTest()
  }

  def unionTest(withCoprocessor: Boolean = true) = {
    val types0 = Seq(IntegerType, IntegerType, StringType)
    val types1 = Seq(IntegerType)

    def generateRowKey0(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types0)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }
    def generateRowKey1(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types1)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    TestHbase.catalog.createHBaseUserTable("teacher", Set("cf"), null, withCoprocessor)
    TestHbase.catalog.createHBaseUserTable("people", Set("cf"), null, withCoprocessor)

    val sql0 =
      s"""CREATE TABLE spark_teacher_3key(
          grade INT, class INT, subject STRING, teacher_name STRING, teacher_age INT,
          PRIMARY KEY(grade, class, subject))
          MAPPED BY (teacher, COLS=[teacher_name=cf.a, teacher_age=cf.b])"""
        .stripMargin
    TestHbase.executeSql(sql0).toRdd.collect()
    val sql1 =
      s"""CREATE TABLE spark_people(
          rowNum INT, people_name STRING, people_age INT,
          school_identification STRING, school_director STRING,
          PRIMARY KEY(rowNum))
          MAPPED BY (people, COLS=[people_name=cf.a, people_age=cf.b,
          school_identification=cf.c, school_director=cf.d])"""
        .stripMargin
    TestHbase.executeSql(sql1).toRdd.collect()

    val inputFile0 = "'" + hbaseHome + "/teacher.txt'"
    val loadSql0 = "LOAD PARALL DATA LOCAL INPATH " + inputFile0 + " INTO TABLE spark_teacher_3key"
    TestHbase.executeSql(loadSql0).toRdd.collect()
    val inputFile1 = "'" + hbaseHome + "/people.txt'"
    val loadSql1 = "LOAD PARALL DATA LOCAL INPATH " + inputFile1 + " INTO TABLE spark_people"
    TestHbase.executeSql(loadSql1).toRdd.collect()

    val sql = "select people_name from spark_people union select teacher_name from spark_teacher_3key"
    checkResult(TestHbase.sql(sql), containExchange = true, 4)

    TestHbase.sql("drop table spark_teacher_3key")
    dropNativeHbaseTable("teacher")
    TestHbase.sql("drop table spark_people")
    dropNativeHbaseTable("people")
  }

  test("SORT TEST") {
    sortTest()
  }

  def sortTest(withCoprocessor: Boolean = true) = {
    val types0 = Seq(IntegerType, IntegerType, StringType)

    def generateRowKey0(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types0)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    TestHbase.catalog.createHBaseUserTable("teacher", Set("cf"), null, withCoprocessor)

    val sql0 =
      s"""CREATE TABLE spark_teacher_3key(
          grade INT, class INT, subject STRING, teacher_name STRING, teacher_age INT,
          PRIMARY KEY(grade, class, subject))
          MAPPED BY (teacher, COLS=[teacher_name=cf.a, teacher_age=cf.b])"""
        .stripMargin
    TestHbase.executeSql(sql0).toRdd.collect()

    val inputFile0 = "'" + hbaseHome + "/teacher.txt'"
    val loadSql0 = "LOAD PARALL DATA LOCAL INPATH " + inputFile0 + " INTO TABLE spark_teacher_3key"
    TestHbase.executeSql(loadSql0).toRdd.collect()

    val result = runSql("select teacher_name from spark_teacher_3key order by grade limit 2")
    val exparr = Array(Array("Zou"), Array("Peter"))
    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    TestHbase.sql("drop table spark_teacher_3key")
    dropNativeHbaseTable("teacher")
  }

  test("TEST Random") {
    TestRandom()
  }

  def TestRandom(withCoprocessor: Boolean = true) = {
    val types0 = Seq(IntegerType, IntegerType, StringType)

    def generateRowKey0(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types0)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    TestHbase.catalog.createHBaseUserTable("teacher", Set("cf"), null, withCoprocessor)

    val sql0 =
      s"""CREATE TABLE spark_teacher_3key(
          grade INT, class INT, subject STRING, teacher_name STRING, teacher_age INT,
          PRIMARY KEY(grade, class, subject))
          MAPPED BY (teacher, COLS=[teacher_name=cf.a, teacher_age=cf.b])"""
        .stripMargin
    TestHbase.executeSql(sql0).toRdd.collect()

    val inputFile0 = "'" + hbaseHome + "/teacher.txt'"
    val loadSql0 = "LOAD PARALL DATA LOCAL INPATH " + inputFile0 + " INTO TABLE spark_teacher_3key"
    TestHbase.executeSql(loadSql0).toRdd.collect()

    val df: DataFrame = TestHbase.table("spark_teacher_3key")
    import org.apache.spark.sql.functions._
    df.select(col("*"), randn(5L)).show()

    TestHbase.sql("drop table spark_teacher_3key")
    dropNativeHbaseTable("teacher")
  }

  def checkResult(df: DataFrame, containExchange: Boolean, size: Int) = {
    df.queryExecution.executedPlan match {
      case a: org.apache.spark.sql.execution.Aggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case a: org.apache.spark.sql.execution.GeneratedAggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case _ => Nil
    }
    assert(df.collect().length == size)
  }
}