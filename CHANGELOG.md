# Change Log

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
