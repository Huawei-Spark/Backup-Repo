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

package org.apache.spark.sql.hbase.api.java;

import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hbase.*;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

public class JavaAPISuite extends TestBase implements Serializable {
    private transient JavaSparkContext sc;
    private transient SQLContext hsc;
    private transient MiniHBaseCluster cluster;
    private transient HBaseAdmin hbaseAdmin;

    private final String hb_staging_table = "HbStagingTable";
    private final String staging_table = "StagingTable";
    private final String create_sql = "CREATE TABLE " + staging_table + "(strcol STRING, bytecol String, shortcol String, intcol String, " +
            "longcol string, floatcol string, doublecol string, PRIMARY KEY(doublecol, strcol, intcol))" +
            " MAPPED BY (" + hb_staging_table + ", COLS=[bytecol=cf1.hbytecol, " +
            "shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])";
    private final String insert_sql = "INSERT INTO TABLE " + staging_table + " VALUES (\"strcol\" , \"bytecol\" , \"shortcol\" , \"intcol\" ," +
            "  \"longcol\" , \"floatcol\" , \"doublecol\")";
    private final String retrieve_sql = "SELECT * FROM " + staging_table;

    @Before
    public void setUp() {
        System.setProperty("spark.hadoop.hbase.zookeeper.quorum", "localhost");

        sc = new JavaSparkContext("local[2]", "JavaAPISuite", new SparkConf(true));
        hsc = new HBaseSQLContext(sc);

        HBaseTestingUtility testUtil = new HBaseTestingUtility(hsc.sparkContext().
                hadoopConfiguration());

        int nRegionServers = 1;
        int nDataNodes = 1;
        int nMasters = 1;

        try {
            cluster = testUtil.startMiniCluster(nMasters, nRegionServers, nDataNodes);
            hbaseAdmin = new HBaseAdmin(hsc.sparkContext().hadoopConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateInsertRetrieveTable() {
        hsc.sql(create_sql).collect();
        hsc.sql(insert_sql).collect();
        Row[] row = hsc.sql(retrieve_sql).collect();

        assert (row[0].toString().equals("[strcol,bytecol,shortcol,intcol,longcol,floatcol,doublecol]"));
    }
}
