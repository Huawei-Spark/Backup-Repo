## Astro: Fast SQL on HBase using SparkSQL
### Please refer to Astro's main site: https://github.com/HuaweiBigData/astro

Apache HBase is a distributed Key-Value store of data on HDFS. It is modeled after Google’s Big Table, and provides APIs to query the data. The data is organized, partitioned and distributed by its “row keys”. Per partition, the data is further physically partitioned by “column families” that specify collections of “columns” of data. The data model is for wide and sparse tables where columns are dynamic and may well be sparse.

Although HBase is a very useful big data store, its access mechanism is very primitive and only through client-side APIs, Map/Reduce interfaces and interactive shells. SQL accesses to HBase data are available through Map/Reduce or interfaces mechanisms such as Apache Hive and Impala, or some “native” SQL technologies like Apache Phoenix. While the former is usually cheaper to implement and use, their latencies and efficiencies often cannot compare favorably with the latter and are often suitable only for offline analysis. The latter category, in contrast, often performs better and qualifies more as online engines; they are often on top of purpose-built execution engines.

Currently Spark supports queries against HBase data through HBase’s Map/Reduce interface (i.e., TableInputFormat). SparkSQL supports use of Hive data, which theoretically should be able to support HBase data access, out-of-box, through HBase’s Map/Reduce interface and therefore falls into the first category of the “SQL on HBase” technologies.

We believe, as a unified big data processing engine, Spark is in good position to provide better HBase support.
