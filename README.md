# Spark HBase

Introduction

## Online Documentation

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
