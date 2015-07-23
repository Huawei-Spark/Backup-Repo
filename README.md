## Astro: Fast SQL on HBase using SparkSQL
### Please refer to Astro's main site: https://github.com/HuaweiBigData/astro

Currently Spark supports queries against HBase data through HBase’s Map/Reduce interface (i.e., TableInputFormat). SparkSQL supports use of Hive data, which theoretically should be able to support HBase data access, out-of-box, through HBase’s Map/Reduce interface and therefore falls into the first category of the “SQL on HBase” technologies.

We believe, as a unified big data processing engine, Spark is in good position to provide better HBase support.
