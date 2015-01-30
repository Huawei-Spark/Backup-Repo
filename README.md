# Spark HBase

Apache HBase is a distributed Key-Value store of data on HDFS. It is modeled after Google’s Big Table, and provides APIs to query the data. The data is organized, partitioned and distributed by its “row keys”. Per partition, the data is further physically partitioned by “column families” that specify collections of “columns” of data. The data model is for wide and sparse tables where columns are dynamic and may well be sparse.

Although HBase is a very useful big data store, its access mechanism is very primitive and only through client-side APIs, Map/Reduce interfaces and interactive shells. SQL accesses to HBase data are available through Map/Reduce or interfaces mechanisms such as Apache Hive and Impala, or some “native” SQL technologies like Apache Phoenix. While the former is usually cheaper to implement and use, their latencies and efficiencies often cannot compare favorably with the latter and are often suitable only for offline analysis. The latter category, in contrast, often performs better and qualifies more as online engines; they are often on top of purpose-built execution engines.

Currently Spark supports queries against HBase data through HBase’s Map/Reduce interface (i.e., TableInputFormat). SparkSQL supports use of Hive data, which theoretically should be able to support HBase data access, out-of-box, through HBase’s Map/Reduce interface and therefore falls into the first category of the “SQL on HBase” technologies.

We believe, as a unified big data processing engine, Spark is in good position to provide better HBase support.

## Online Documentation

Online documentation can be found on [Spark JIRA page](https://issues.apache.org/jira/browse/SPARK-3880).

## Building Spark HBase

Spark HBase is built using [Apache Maven](http://maven.apache.org/).

The refactoring job of separating the spark hbase sub-project from the spark project is ongoing.
Some manual steps are required to build the new stand-alone spark-hbase project until this task
is complete.

In an effort the avoid confusion over the terms spark, spark-hbase, and hbase, these two projects
are referred to here as

	"Spark-Huawei/spark":  https://github.com/Huawei-Spark/spark.git  (spark + all sub-modules)
	"Spark-Huawei/hbase":  https://github.com/Huawei-Spark/hbase.git  (standalone spark-hbase project)

In short, you will need to manually delete the spark/sql/hbase module from the Spark-Huawei/spark
source tree, all references to it in the spark build infrastructure, build/install spark, then build
the standalone Spark-Huawei/hbase project.


Here is the step-by-step process:


1. Clone, edit, build Spark-Huawei/spark

Define a SPARK_HOME environment variable on your development machine and clone the project to that location.

    $ git clone https://github.com/Huawei-Spark/spark.git

Change your current working dir to your SPARK_HOME and make sure you downloaded branch 'hbase'.

    $ git branch
    output:  * hbase

Manually remove the sql/hbase module from the Spark-Huawei/spark project.

    $ rm -rf $SPARK_HOME/sql/hbase

Edit the spark project's parent pom.xml -- delete the line '<module>sql/hbase</module>' (from two locations).

Build and install Spark-Huawei/spark; it must be installed in your local maven repo.

    $ mvn -e -T1C -Pyarn,hadoop-2.4,hive  -Dhadoop.version=2.4.0 -DskipTests  clean package install

2. Clone and build Spark-Huawei/hbase (new standalone spark-hbase project)

Change your current working dir to ../$SPARK_HOME and clone the standalone spark-hbase project.

    $ git clone https://github.com/Huawei-Spark/hbase.git

	Make sure you downloaded branch 'master'.

    $ git branch
    output:  * master

You have installed spark in your local maven repo; now you can build Spark-Huawei/hbase against it.

    $ mvn -e -T1C -Phbase,hadoop-2.4  -Dhadoop.version=2.4.0 -DskipTests    clean package install

3. Run Spark-Huawei/hbase test suites against an HBase minicluster, from Maven.

    $ mvn -e -T1C -Phbase,hadoop-2.4  -Dhadoop.version=2.4.0  test


## Interactive Scala Shell

The easiest way to start using Spark HBase is through the Scala shell:

    ./bin/hbase-sql


## Running Tests

Testing first requires [building Spark HBase](#building-spark). Once Spark HBase is built, tests
can be run using:

    ./dev/run-tests

Run all test suites from Maven:

    mvn -Phbase,hadoop-2.4 test

Run a single test suite from Maven, for example:

    mvn -Phbase,hadoop-2.4 test -DwildcardSuites=org.apache.spark.sql.hbase.BasicQueriesSuite

## IDE Setup

We use IntelliJ IDEA for Spark HBase development. You can get the community edition for free and install the JetBrains Scala plugin from Preferences > Plugins.

To import the current Spark HBase project for IntelliJ:

1. Download IntelliJ and install the Scala plug-in for IntelliJ. You may also need to install Maven plug-in for IntelliJ.
2. Go to "File -> Import Project", locate the Spark HBase source directory, and select "Maven Project".
3. In the Import Wizard, select "Import Maven projects automatically" and leave other settings at their default. 
4. Make sure some specific profiles are enabled. Select corresponding Hadoop version, "maven3" and also"hbase" in order to get dependencies.
5. Leave other settings at their default and you should be able to start your development.
6. When you run the scala test, sometimes you will get out of memory exception. You can increase your VM memory usage by the following setting, for example:

```
    -XX:MaxPermSize=512m -Xmx3072m
```

You can also make those setting to be the default by setting to the "Defaults -> ScalaTest".

## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.
