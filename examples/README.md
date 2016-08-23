# Examples
The Spark-Riak connector comes with several sample programs and demos:

- [Simple Scala example](#simple-scala-example)
- [Simple Scala Riak TS example](#simple-scala-riak-ts-example)
- [Simple Scala DataFrame example](#simple-scala-dataframe-example)
- [Simple Scala Riak TS DataFrame example](#simple-scala-riak-ts-dataframe-example)
- [Simple Java example](#simple-java-example)
- [Simple Java Riak TS example](#simple-java-riak-ts-example)
- [OFAC demo](#ofac-demo)
- [Scala RiakTS Parquet Example](#scala-riak-ts-parquet-example)
- [Spark Streaming Examples](#spark-streaming-examples)
  - [Spark Streaming Scala Riak KV Example](#spark-streaming-riak-kv-example)
  - [Spark Streaming Scala Riak TS Example](#spark-streaming-riak-ts-example)

## Building and Running Examples/Demos

1. All of the examples assume that you have a Riak KV or Riak TS cluster installed and running on localhost:8087. You can follow these guides to setup a Riak KV or Riak TS cluster: [Installing Riak KV](http://docs.basho.com/riak/kv/latest/setup/installing/) and [Installing Riak TS](http://docs.basho.com/riak/ts/latest/installing/).

2. If you don't have SBT installed, go to the [SBT download page](http://www.scala-sbt.org/download.html) and follow the installation instructions for your OS.

3. Then, install the dependencies (we will skip integration tests to speed things up):
```
sbt clean package assembly
```

4. Go to examples repl folder:
```
cd examples/src/main/repl
```

5. Update conf/config.sh file with your settings if needed

6. Run the example or demo that you want by running `bin/run-example <class>`. For example: `./bin/run-example SimpleScalaRiakExample` will run the SimpleScalaRiakExample example locally.



## Simple Scala Example

This Scala example demonstrates how to use Riak Spark connector to query all data from the corresponding bucket.
This example creates test data in the Riak, pulls it back to Spark by utilizing RiakRDD, and as a result,
calculates the number of values loaded from the Riak bucket.

Run it locally:
```
bin/run-example SimpleScalaRiakExample
```

Sources [SimpleScalaRiakExample.scala](./src/main/scala/com/basho/riak/spark/examples/SimpleScalaRiakExample.scala)



## Simple Scala Riak TS Example

This Scala example demonstrates how to use Riak Spark connector to do range queries in TS and how to do the same in KV

Before running, you will need to create and activate a TS table called `ts_weather_demo` if it does not already exist. You can find more information on creating and activating TS tables [here](http://docs.basho.com/riak/ts/latest/using/creating-activating/), or you can run the following:

```
riak-admin bucket-type create ts_weather_demo '{"props":{"n_val":3, "table_def": "CREATE TABLE ts_weather_demo (weather VARCHAR NOT NULL, family VARCHAR NOT NULL, time TIMESTAMP NOT NULL, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, PRIMARY KEY ((weather, family, QUANTUM(time, 1, 'h')), weather, family, time))"}}'

riak-admin bucket-type activate ts_weather_demo
```

You can run the example locally with:
```
bin/run-example SimpleScalaRiakTSExample
```

Sources [SimpleScalaRiakTSExample.scala](./src/main/scala/com/basho/riak/spark/examples/SimpleScalaRiakTSExample.scala)



## Simple Scala DataFrame Example

This Scala example demonstrates how to use Spark Dataframes with RiakKV 

You can run the example locally with:
```
bin/run-example dataframes.SimpleScalaRiakDataframesExample
```

Sources [SimpleScalaRiakDataframesExample.scala](./src/main/scala/com/basho/riak/spark/examples/dataframes/SimpleScalaRiakDataframesExample.scala)


## Simple Scala Riak TS DataFrame example

This Scala example demonstrates how to use Spark Dataframes with Riak TS 

Before running, you will need to create and activate a TS table called `ts_weather_demo` if it does not already exist. You can find more information on creating and activating TS tables [here](http://docs.basho.com/riak/ts/latest/using/creating-activating/), or you can run the following:

```
riak-admin bucket-type create ts_weather_demo '{"props":{"n_val":3, "table_def": "CREATE TABLE ts_weather_demo (weather VARCHAR NOT NULL, family VARCHAR NOT NULL, time TIMESTAMP NOT NULL, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, PRIMARY KEY ((weather, family, QUANTUM(time, 1, 'h')), weather, family, time))"}}'

riak-admin bucket-type activate ts_weather_demo
```

You can run the example locally with:
```
bin/run-example dataframes.SimpleScalaRiakTSDataframesExample
```

Sources [SimpleScalaRiakTSDataframesExample.scala](./src/main/scala/com/basho/riak/spark/examples/dataframes/SimpleScalaRiakTSDataframesExample.scala)



## Simple Java example

This Java example demonstrates how to use Riak Spark connector to query query Riak KV.
This example creates test data in the Riak, pulls it back to Spark by utilizing such features as full bucket read, 2i range query, 2i keys query, and query by keys.

Run it locally:
```
bin/run-example SimpleJavaRiakExample
```
Sources [SimpleJavaRiakExample.java](./src/main/java/com/basho/riak/spark/examples/SimpleJavaRiakExample.java)



## Simple Java Riak TS example

This Java example demonstrates how to use Riak Spark connector to query query Riak TS.

This example creates test data in the Riak, pulls it back to Spark by utilizing range scan query.

Before running, you will need to create and activate a TS table called `ts_weather_demo` if it does not already exist. You can find more information on creating and activating TS tables [here](http://docs.basho.com/riak/ts/latest/using/creating-activating/), or you can run the following:

```
riak-admin bucket-type create ts_weather_demo '{"props":{"n_val":3, "table_def": "CREATE TABLE ts_weather_demo (weather VARCHAR NOT NULL, family VARCHAR NOT NULL, time TIMESTAMP NOT NULL, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, PRIMARY KEY ((weather, family, QUANTUM(time, 1, 'h')), weather, family, time))"}}'

riak-admin bucket-type activate ts_weather_demo
```

You can run the example locally with:
```
bin/run-example SimpleJavaRiakTSExample
```
Sources [SimpleJavaRiakExample.java](./src/main/java/com/basho/riak/spark/examples/SimpleJavaRiakTSExample.java)



## OFAC demo

This demo shows how Riak and Spark can be used to analyze semi-structured data using Scala.

As part of its enforcement efforts, the Office of Foreign Assets Control (OFAC) publishes a list of individuals and companies owned, controlled by, or acting for/on behalf of targeted countries. It also lists individuals, groups, and entities, such as terrorists and narcotics traffickers designated under programs that are not country-specific. Collectively, such individuals and companies are called "Specially Designated Nationals” (SDNs). Their assets are blocked and U.S. persons are generally prohibited from dealing with them.

More information about OFAC list can be found at [www.treasury.gov](http://www.treasury.gov/resource-center/sanctions/SDN-List/Pages/default.aspx)

### Initial Data
All the data required for this demo will be downloaded automatically from the public internet copies.

We will download a public copy of the [SDN list from OFAC](http://www.treasury.gov/ofac/downloads/sdn.csv). In order to connect SDNs to their specific locations, we will be downloading an [address list](http://www.treasury.gov/ofac/downloads/add.csv). And, finally, we will download a [Stop words list](https://en.wikipedia.org/wiki/Stop_words) for the proper word frequency analysis.

### Calculated Metrics
In the demo we are going to generate descriptive and summary statistics from the OFAC dataset. As a part of our analysis we are going to calculate the following:

* How many unique SDNs are there in total? (see output during execution)
* Which distinct SDN Types does this dataset contain? (see output during execution)
* How many banned individuals per country are in OFAC list? (supporting chart: bar plot)
* How many marine vessels are on the list by country and by vessel type? (supporting chart: heat map)
* What is the probability distribution of the vessel tonnage for vessels on the list? (supporting chart: histogram)
* What are the most common titles of the individuals on the list? (see output during execution)

You can run the demo locally by:
```
bin/run-example demos.ofac.OFACDemo
```

Sources [OFACDemo.scala](./src/main/scala/com/basho/riak/spark/examples/demos/ofac/OFACDemo.scala)


## Scala Riak TS Parquet Example

Simple demo which illustrates how data can be extracted from Riak TS and saved as a parquet file.

Before running, you will need to create and activate a TS table called `parquet_demo` if it does not already exist. You can find more information on creating and activating TS tables [here](http://docs.basho.com/riak/ts/latest/using/creating-activating/), or you can run the following:

```
riak-admin bucket-type create parquet_demo '{"props":{"n_val":3, "table_def":"CREATE TABLE parquet_demo (site VARCHAR NOT NULL, species VARCHAR NOT NULL, measurementDate TIMESTAMP NOT NULL, latitude DOUBLE, longitude DOUBLE, value DOUBLE, PRIMARY KEY ((site, species, QUANTUM(measurementDate, 24, h)),site, species, measurementDate))"}}'

riak-admin bucket-type activate parquet_demo
```

You can run the example locally with:
```
bin/run-example parquet.ScalaRiakParquetExample
```

Sources [ScalaRiakParquetExample.scala](./src/main/scala/com/basho/riak/spark/examples/parquet/ScalaRiakParquetExample.scala)


## Spark Streaming Examples

The Spark-Riak Connector can be used with Spark Streaming. To demonstrate this usage, we will work through two small Scala examples, one for Riak KV and the other for Riak TS.

These examples require the use of Kafka. Please install Kafka and setup a Kafka broker prior to running this example. We will assume that there is a Kafka broker running at `127.0.0.1:9092`. Instructions for setting up Kafka topics can be found in [this guide](https://kafka.apache.org/documentation.html#quickstart). You can create a broker with the following:

```
path/to/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties
path/to/kafka/bin/kafka-server-start.sh config/server.properties
```

We also assume Riak KV/TS is installed and there is a Riak KV/TS node running at `127.0.0.1:8087`. You can find instruction to do so [here](http://docs.basho.com/riak/ts/latest/installing/). 

Riak KV, Kafka and Spark master hostnames must be specified in [config.sh](./src/main/repl/conf/config.sh) prior to running the examples.

**Important Note**: [config.sh](./src/main/repl/conf/config.sh) will attempt to gather your local scala version. This version number will be used in [run-example](./src/main/repl/bin/run-example) to pull the appropriate `spark-streaming-kafka` and `kafka` libraries from spark-packages.org. This can cause an error if your local Spark and the Spark-Riak-Connector were built with a different Scala version than your local Scala. For example, Spark 1.6.2 is built with Scala 2.10 by default. Therefore, in order for the streaming examples to work, the Scala version [config.sh](./src/main/repl/conf/config.sh) picks up must be the same as the Scala version that Spark and the Spark-Riak-Connector were built with. If your local Scala version is different than the version that Spark and the Spark-Riak-Connector were built with, you should change the Scala version in [this line](src/main/repl/conf/config.sh#L53) in [config.sh](./src/main/repl/conf/config.sh) to the version of Scala that Spark and the Spark-Riak-Connector were built with.


### Spark Streaming Riak KV Example

This example will start a stream from the Kafka topic `ingest-kv` into the KV bucket `test-data`. This stream will run until terminated. Whenever a message is produced for Kafka topic `ingest-kv`, the Spark Streaming context will automatically stream the message from the topic into the KV bucket. To see this in action, we first need create the `ingest-kv` topic:

```
path/to/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ingest-kv
```

Then, start the example:

```
bin/run-example streaming.StreamingKVExample
```

Next, we need to send a message to the Kafka topic `ingest-kv` with the Kafka console producer script, which can be found in the Kafka directory:

```
/path/to/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ingest-kv
```
 
This script will read messages from the terminal and pass it to the topic. From the topic, the Spark Streaming context will write the message to Riak KV bucket `test-data`.  As an example put the following into the terminal:
 
```
{"time": "2016-01-01 08:30:00.000", "weather": "sunny", "temperature": 25.0, "humidity": 67.0, "pressure": 30.20, "family": "f"}
```
 
You should now be able to see this data entry in the KV bucket `test-data`.

Sources [StreamingKVExample.scala](./src/main/scala/com/basho/riak/spark/examples/streaming/StreamingKVExample.scala)

### Spark Streaming Riak TS Example

Having seen how Spark Streaming works with KV buckets, let's now look at the TS table example.

This example will start a stream from the Kafka topic `ingest-ts` into the TS table `ts_weather_demo`. This stream will run until terminated. Whenever a message is produced for Kafka topic `ingest-ts`, the Spark Streaming context will automatically stream the message from the topic into the TS table. To see this in action, we first need to create and activate the TS table. You can find more information on creating and activating TS tables [here](http://docs.basho.com/riak/ts/latest/using/creating-activating/). For this demo we will create and activate the table with the following:

```
riak-admin bucket-type create ts_weather_demo '{"props":{"n_val":3, "table_def": "CREATE TABLE ts_weather_demo (weather VARCHAR NOT NULL, family VARCHAR NOT NULL, time TIMESTAMP NOT NULL, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, PRIMARY KEY ((weather, family, QUANTUM(time, 1, 'h')), weather, family, time))"}}'

riak-admin bucket-type activate ts_weather_demo
```

Now that we have created a TS table, we need to create the `ingest-ts` topic:

```
path/to/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ingest-ts
```

Now, we can run the `StreamingTSExample.scala` example with:

```
bin/run-example streaming.StreamingTSExample
```

Now that the stream is up and running, we need to send data to the Kafka topic. Let's start the Kafka console producer. This will allow us to stream messages from the terminal into the Kafka `ingest-ts` topic.

```
/path/to/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ingest-ts
```
 
Now paste the following message into the terminal:

```
{"time": "2016-01-01 08:30:00.000", "weather": "sunny", "temperature": 25.0, "humidity": 67.0, "pressure": 30.20, "family": "f"}
```

You can check that this worked by doing a simple SQL query for the example data. 

Sources [StreamingTSExample.scala](./src/main/scala/com/basho/riak/spark/examples/streaming/StreamingTSExample.scala)
