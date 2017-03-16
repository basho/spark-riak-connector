# Change Log

## [1.6.3](https://github.com/basho/spark-riak-connector/releases/tag/v1.6.3)

* Data locality support for Coverage Plan Based Partitioning [#230](https://github.com/basho/spark-riak-connector/pull/230)
* Miscellaneous enhancements and bug fixes
    * Improve Coverage Plan Based Partitioning: smart split calculation and more accurate coverage entry distribution across the partitions [#231](https://github.com/basho/spark-riak-connector/pull/231) 
    * Critical, Fix Python serialization for empty JSON objects [#226](https://github.com/basho/spark-riak-connector/pull/226)
    * Fix double filtering for DataFrames [#228](https://github.com/basho/spark-riak-connector/pull/228)

## [1.6.2](https://github.com/basho/spark-riak-connector/releases/tag/v1.6.2)
Critical fix Python KV:  if object values are JSON objects with list fields (empty or not) then exception happens (https://github.com/basho/spark-riak-connector/pull/219).

## [1.6.1](https://github.com/basho/spark-riak-connector/releases/tag/v1.6.1)
Miscellaneous enhancements and bug fixes
    * Make Namespace not to be mandatory for KV operations (https://github.com/basho/spark-riak-connector/pull/212),(https://github.com/basho/spark-riak-connector/pull/203)
    * Make the Python tests be executed on TravisCI for both: TS and KV (https://github.com/basho/spark-riak-connector/pull/213)
    * Critical, Fix improper Spark partition order (https://github.com/basho/spark-riak-connector/pull/211)
    * Critical, Fix Python serialization for JSON values with more than 4 keys (https://github.com/basho/spark-riak-connector/pull/210)
    * Critical, fix empty writes (https://github.com/basho/spark-riak-connector/pull/205)
    * Fix value mapping for JSON array values on KV reads (https://github.com/basho/spark-riak-connector/pull/215)  
    * Fix Python .partitionBy2iKeys() error (https://github.com/basho/spark-riak-connector/pull/216)
    

## [1.6.0](https://github.com/basho/spark-riak-connector/releases/tag/v1.6.0)

* Support Python and KV buckets
* Support Spark Streaming context in the connector
* When a Riak node is unavailable during a Spark job, failover to another node
* Remove separate java-connector and incorporate java classes into main connector project
* Build with SBT instead of Maven
* Build artifacts for Scala 2.10 and 2.11
* Use Docker for builds and tests
* Update docs and examples
* Miscellaneous enhancements and bug fixes
    * Improve TS Partitioning [#96](https://github.com/basho/spark-riak-connector/pull/96)
    * Query2iRange parallelization for KV [#146](https://github.com/basho/spark-riak-connector/pull/146)
    * Allow passing quanta manually [#153](https://github.com/basho/spark-riak-connector/pull/153)
    * Connector stuck on application exit [#164](https://github.com/basho/spark-riak-connector/pull/164)
    * Fix TSDataQueryingIterator issue with breaking iteration on the first sub-query returned no results [#178](https://github.com/basho/spark-riak-connector/pull/178)
    * Fix irregular data processing [#190](https://github.com/basho/spark-riak-connector/pull/190)


## [1.5.1](https://github.com/basho/spark-riak-connector/releases/tag/v1.5.1)

* Make the Spark Riak connector available in Maven Central

## [1.5.0](https://github.com/basho/spark-riak-connector/releases/tag/v1.5.0) (2016-04-15)

* Reorganize and expand documentation 
* Add bulk size parameter for writes to Riak TS table (spark.riakts.write.bulk-size) [#129](https://github.com/basho/spark-riak-connector/pull/129)
* Rename riakTSBucket() to riakTSTable() [#128](https://github.com/basho/spark-riak-connector/pull/128)
* Update Spark version to 1.6.1 [#109](https://github.com/basho/spark-riak-connector/pull/109)
* Add support for Spark partitioning with Riak TS tables [#96](https://github.com/basho/spark-riak-connector/pull/96)
* Miscellaneous bug fixes
