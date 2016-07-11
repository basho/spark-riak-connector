# Building And Testing The Spark-Riak Connector

If you want to download the source code of the Spark-Riak connector, build it, and install the results in your local repo, this is the document for you! Keep reading for instructions on downloading, building, and installing the connector.

If you just want to add the connector as a dependency to your application, you'll want to go [here](https://github.com/basho/spark-riak-connector/tree/master/documentation/add-as-dependecy.md) instead.


## Prerequisites

In order to build the Spark-Riak connector, you'll need to have the following installed: 

* [Java OpenJDK 8](http://openjdk.java.net/install/)
* [Maven 3](https://maven.apache.org/download.cgi)
* [Spark 1.6](http://spark.apache.org/docs/latest/#downloading)
* [Riak KV](http://docs.basho.com/riak/kv/2.2.0/setup/installing/) or [Riak TS](http://docs.basho.com/riak/ts/latest/installing/)


## Download

Once you've installed all of the prerequisites, you need to clone the Spark-Riak connector GitHub repository. 

Make sure you've navigated to the directory you want the Spark-Riak connector to be in, then run:

```
git clone https://github.com/basho/spark-riak-connector.git
```

## Build

After cloning this repository, you can build the Spark-Riak connector:

```
sbt clean package
```

Once the connector is built there are several jars that are produced:
`spark-riak-connector/target/` contains `spark-riak-connector-{{version}}.jar` - this is the connector jar.
 
To create uber jar you should use assembly task:

```
sbt assembly
```
As a result uber jar will be produced.
`spark-riak-connector/target/` will contain `spark-riak-connector-{{version}}-uber.jar` - this is the connector uber jar.

For publishing connector locally you can use publish-local task:

```
sbt publish-local
```
You'll find the results of the build in your local ivy2 repository in the com/basho/riak directory. Most likely that will be in your home directory and the path will look like this: `~/.ivy2/local/com/basho/riak/`

You can publish connector to Maven local repo using publishM2 task:
```
sbt publishM2
```
You'll find the results of the build in your local maven repository in the com/basho/riak directory. Most likely that will be in your home directory and the path will look like this: ~/.m2/repository/com/basho/riak/


## Test

For the Spark-Riak Connector, unit tests are separated from integration tests. 

To run all test use test task:
```
sbt clean test
```

To run specific tests you should use command aliases: 
```
sbt runIntegrationTests
sbt runRegressionTests
sbt runRiakKVTests
sbt runRiakTSTests
sbt runNonIntegrationTests
``` 