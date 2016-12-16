# Spark-Riak Connector [![Build Status](https://travis-ci.org/basho/spark-riak-connector.svg?branch=develop)](https://travis-ci.org/basho/spark-riak-connector)

The Spark-Riak connector enables you to connect Spark applications to Riak KV and Riak TS with the Spark RDD and Spark DataFrames APIs. You can write your app in Scala, Python, and Java. The connector makes it easy to partition the data you get from Riak so multiple Spark workers can process the data in parallel and it has support for failover if a Riak node goes down while your Spark job is running.

## Features

* Construct a Spark RDD from a Riak KV bucket with a set of keys
* Construct a Spark RDD from a Riak KV bucket by using a 2i string index or a set of indexes
* Construct a Spark RDD from a Riak KV bucket by using a 2i range query or a set of ranges
* Map JSON formatted data from Riak KV to user defined types
* Save a Spark RDD into a Riak KV bucket and apply 2i indexes to the contents
* Construct a Spark Dataframe from a Riak TS table using range queries and schema discovery
* Save a Spark Dataframe into a Riak TS table
* Construct a Spark RDD using Riak KV bucket's enhanced 2i query (a.k.a. full bucket read)
* Perform parallel full bucket reads from a Riak KV bucket into multiple partitions

## Compatibility

* Riak TS 1.3.1+
* Apache Spark 1.6+
* Scala 2.10 and 2.11
* Java 8

## Coming Soon

* Support for Riak KV 2.3 and later

## Prerequisites

In order to use the Spark-Riak connector, you must have the following installed:

* [Java OpenJDK 8](http://openjdk.java.net/install/) or [Oracle JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Apache Spark](http://spark.apache.org/docs/latest/#downloading)
* [Riak KV](http://docs.basho.com/riak/kv/latest) or [Riak TS](http://docs.basho.com/riak/ts/latest/)

## Spark-Riak Connector

- [Quick Start](./docs/quick-start.md#quick-start-guide)

- [Getting the Spark-Riak Connector](./docs/getting-connector.md#getting-the-spark-riak-connector)
  - [Scala](./docs/getting-connector.md#scala)
  - [Python](./docs/getting-connector.md#python)
  - [Java](./docs/getting-connector.md#java)
  - [Alternative Methods](./docs/getting-connector.md#alternative-scala-java-python)

- [Building and Testing Spark-Riak Connector](./docs/building-and-testing-connector.md#building-and-testing-the-spark-riak-connector)

- [Using the Spark-Riak Connector](./docs/using-connector.md#using-the-spark-riak-connector)
  - [Configuration of Spark Context](./docs/using-connector.md#configuration-of-spark-context)
  - [Failover Handling](./docs/using-connector.md#failover-handling)
  - [Reading Data From KV Bucket](./docs/using-connector.md#reading-data-from-kv-bucket)
  - [Writing Data To KV Bucket](./docs/using-connector.md#writing-data-to-kv-bucket)
  - [Writing Data To KV Bucket With 2i Indices](./docs/using-connector.md#writing-data-to-kv-bucket-with-2i-indices)
  - [Reading Data From TS Table](./docs/using-connector.md#reading-data-from-ts-table)
  - [Writing Data To TS Table](./docs/using-connector.md#writing-data-to-ts-table)
  - [Spark Dataframes With KV Bucket](./docs/using-connector.md#spark-dataframes-with-kv-bucket)
  - [Spark Dataframes With TS Table](./docs/using-connector.md#spark-dataframes-with-ts-table)
  - [Partitioning For KV Buckets](./docs/using-connector.md#partitioning-for-kv-buckets)
  - [Working With TS Dates](./docs/using-connector.md#working-with-ts-dates)
  - [Partitioning for Riak TS Table Queries](./docs/using-connector.md#partitioning-for-riak-ts-table-queries)
  - [TS Bulk Write](./docs/using-connector.md#ts-bulk-write)
  - [Using Jupyter Notebook](./docs/using-connector.md#using-jupyter-notebook)
  - [Spark Streaming](./docs/using-connector.md#spark-streaming)
  - [Using Java With The Connector](./docs/using-connector.md#using-java-with-the-connector)

- [Spark-Riak Connector Example Apps](./examples/README.md#examples-and-interactive-scala-shell-repl)

## Mailing List

The [Riak Users Mailing List](http://lists.basho.com/mailman/listinfo/riak-users_lists.basho.com) is highly trafficked and a great resource for technical discussions, Riak issues and questions, and community events and announcements.

We pride ourselves on answering every email that comes over the Riak User mailing list. [Sign up](http://lists.basho.com/mailman/listinfo/riak-users_lists.basho.com) and send away. If you prefer points for your questions, you can always [tag Riak on StackOverflow](https://stackoverflow.com/questions/tagged/riak).

## IRC

The [#riak IRC room on irc.freenode.net](https://irc.lc/freenode/riak) is a great place for real-time help with your Riak issues and questions.

## Reporting Bugs

To report a bug or issue, please open a [new issue](https://github.com/basho/spark-riak-connector/issues) against this repository.

You can read the full guidelines for bug reporting on the [Riak Docs](http://docs.basho.com/community/reporting-bugs/).

## Contributing

Basho encourages contributions to the Spark-Riak Connector from the community. Here’s how to get started.

* Fork the appropriate project that is affected by your change.
* Make your changes and run the test suite.
* Commit your changes and push them to your fork.
* Open pull-requests for the appropriate projects.
* Basho engineers will review your pull-request, suggest changes, and merge it when it’s ready and/or offer feedback.

## License

Copyright © 2016 Basho Technologies

Licensed under the Apache License, Version 2.0
