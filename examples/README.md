# Examples and Interactive Scala Shell (REPL)
Riak Spark connector comes with several sample programs and demos:

- [Simple Scala example](#simple-scala-example)
- [Simple Scala RiakTS example](#simple-scala-riakts-example)
- [Simple Scala DataFrame example](#simple-scala-dataframe-example)
- [Simple Scala RiakTS DataFrame example](#simple-scala-riakts-dataframe-example)
- [Simple Java example](#simple-java-example)
- [Simple Java RiakTS example](#simple-java-riakts-example)
- [OFAC demo](#ofac-demo)
- [Scala RiakTS Parquet Example](#scala-riakts-parquet-example)
- [Streaming Scala RiakKV Example](#streaming-scala-riakkv-example)
- [Streaming Scala RiakTS Example](#streaming-scala-riakts-example)

It also comes with a helper for running a ready-to-go [bootstrapped Spark shell](#interactive-scala-shell).

## Building and Running Examples/Demos
1. All of the examples assume that you have a Riak KV or Riak TS cluster installed and running on localhost:8087. You can follow these guides to setup a Riak KV or Riak TS cluster: [Installing Riak KV](http://docs.basho.com/riak/kv/2.1.4/setup/installing/) and [Installing Riak TS](http://docs.basho.com/riak/ts/1.2.0/installing/).

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

## Interactive Scala Shell
The easiest way to start using Spark is through the Scala shell. You can begin using the Scala shell by running:
```
bin/rspark-shell
```

The original Spark shell will run, bootstrapped with all necessary [imports](./src/main/repl/conf/rspark-shell-defaults.scala) and proper classpath.

## Simple Scala Example
This Scala example demonstrates how to use Riak Spark connector to query all data from the corresponding bucket.
This example creates test data in the Riak, pulls it back to Spark by utilizing RiakRDD, and as a result,
calculates the number of values loaded from the Riak bucket.

Run it locally:
```
bin/run-example SimpleScalaRiakExample
```

Sources [SimpleScalaRiakExample.scala](./src/main/scala/com/basho/riak/spark/examples/SimpleScalaRiakExample.scala)

## Simple Scala RiakTS Example
This Scala example demonstrates how to use Riak Spark connector to do range queries in TS and how to do the same in KV

Run it locally:
```
bin/run-example SimpleScalaRiakTSExample
```

Sources [SimpleScalaRiakTSExample.scala](./src/main/scala/com/basho/riak/spark/examples/SimpleScalaRiakTSExample.scala)

## Simple Scala DataFrame Example
This Scala example demonstrates how to use Spark Dataframes with RiakKV 

Run it locally:
```
bin/run-example dataframes.SimpleScalaRiakDataframesExample
```

Sources [SimpleScalaRiakDataframesExample.scala](./src/main/scala/com/basho/riak/spark/examples/dataframes/SimpleScalaRiakDataframesExample.scala)

## Simple Scala RiakTS DataFrame example
This Scala example demonstrates how to use Spark Dataframes with Riak TS 

Run it locally:
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

## Simple Java RiakTS example
This Java example demonstrates how to use Riak Spark connector to query query Riak TS.
This example creates test data in the Riak, pulls it back to Spark by utilizing range scan query.

Run it locally:
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

## Scala RiakTS Parquet Example
Simple demo which illustrates how data can be extracted from Riak TS and saved as a parquet file 

Run it locally:
```
bin/run-example parquet.ScalaRiakParquetExample
```

Sources [ScalaRiakParquetExample.scala](./src/main/scala/com/basho/riak/spark/examples/parquet/ScalaRiakParquetExample.scala)

## Streaming Scala RiakKV Example
Simple demo for Spark streaming job integration. For correct execution:
*   kafka broker must be installed and running;
*   'streaming' topic must be created;
*   Riak KV, kafka and spark master hostnames must be specified in [config.sh](./src/main/repl/conf/config.sh) 

Run it locally:
```
bin/run-example streaming.StreamingKVExample
```
This will start Spark streaming job waiting for your kafka messages.

Sources [StreamingKVExample.scala](./src/main/scala/com/basho/riak/spark/examples/streaming/StreamingKVExample.scala)

## Streaming Scala RiakTS Example
Simple demo for Spark streaming job integration. For correct execution:
*   kafka broker must be installed and running;
*   'streaming' topic must be created;
*   Riak TS, kafka and spark master hostnames must be specified in [config.sh](./src/main/repl/conf/config.sh) 

Run it locally:
```
bin/run-example streaming.StreamingTSExample
```
This will start Spark streaming job waiting for your messages. Message format should be the same as in the example below:
```
{"time": "2016-01-01 08:30:00.000", "weather": "sunny", "temperature": 25.0, "humidity": 67.0, "pressure": 30.20, "family": "f"}
```

Sources [StreamingKVExample.scala](./src/main/scala/com/basho/riak/spark/examples/streaming/StreamingKVExample.scala)

## Run Python examples in Jupyter notebook
To run Python examples in [Jupyter](http://jupyter.readthedocs.io/en/latest/index.html) you need:

* Running Jupyter (you can follow [these](http://jupyter.readthedocs.io/en/latest/install.html) instructions or use official Docker [image](https://hub.docker.com/r/jupyter/pyspark-notebook/))
* Upload your .ipynb file
* Update all connection properties like Riak host and port
* Provide valid path to spark-riak-connector.jar:
```
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/dev/spark-riak-connector-1.5.2-SNAPSHOT-uber.jar pyspark-shell'
```
* Run your Python code