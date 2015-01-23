# Spark HBase

Introduction

## Online Documentation

Apache HBase is a distributed Key-Value store of data on HDFS. It is modeled after Google’s Big Table, and provides APIs to query the data. The data is organized, partitioned and distributed by its “row keys”. Per partition, the data is further physically partitioned by “column families” that specify collections of “columns” of data. The data model is for wide and sparse tables where columns are dynamic and may well be sparse.

Although HBase is a very useful big data store, its access mechanism is very primitive and only through client-side APIs, Map/Reduce interfaces and interactive shells. SQL accesses to HBase data are available through Map/Reduce or interfaces mechanisms such as Apache Hive and Impala, or some “native” SQL technologies like Apache Phoenix. While the former is usually cheaper to implement and use, their latencies and efficiencies often cannot compare favorably with the latter and are often suitable only for offline analysis. The latter category, in contrast, often performs better and qualifies more as online engines; they are often on top of purpose-built execution engines.

Currently Spark supports queries against HBase data through HBase’s Map/Reduce interface (i.e., TableInputFormat). SparkSQL supports use of Hive data, which theoretically should be able to support HBase data access, out-of-box, through HBase’s Map/Reduce interface and therefore falls into the first category of the “SQL on HBase” technologies.

We believe, as a unified big data processing engine, Spark is in good position to provide better HBase support.

Online Documentation can be found on [Spark JIRA page](https://issues.apache.org/jira/browse/SPARK-3880).

Documentation

## Building Spark HBase

Spark HBase is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    mvn -DskipTests clean package


## Interactive Scala Shell

The easiest way to start using Spark HBase is through the Scala shell:

    ./bin/hbase-sql


## Running Tests

Testing first requires [building Spark HBase](#building-spark). Once Spark HBase is built, tests
can be run using:

    ./dev/run-tests

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-with-maven.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions. See also
["Third Party Hadoop Distributions"](http://spark.apache.org/docs/latest/hadoop-third-party-distributions.html)
for guidance on building a Spark application that works with a particular
distribution.

## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.
