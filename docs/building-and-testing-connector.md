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
mvn clean install
```
When you run `install`, the integration tests will execute with the Maven Failsafe Plugin. This ensures the Spark-Riak connector can be built even if there is no Riak cluster running.

To turn on streaming values support for PEX, a special maven profile, "pex_streaming_vals", should be activated. It will make Full Bucket Reads (FBR) more efficient: values will be streamed as a part of the FBR response instead of being fetched in a separate operations. This feature is supported only for Riak TS.
```
mvn clean install -P pex_streaming_vals
```
Or
```
mvn clean install -Dpex_streaming_vals
```
The following command should be used to skip tests:
```
mvn clean install -DskipTests
```

Once the connector is built there are several jars that are produced:
`spark-riak-connector/target/` contains `spark-riak-connector-{{version}}.jar` - this is the connector jar. 

You'll find the results of the build in your local maven repository in the com/basho/riak directory. Most likely that will be in your home directory and the path will look like this: `~/.m2/repository/com/basho/riak/`


## Test

For the Spark-Riak Connector, unit tests are separated from integration tests. 
If there is no Riak installation running, it is still possible to successfully run unit tests:
```
mvn clean test
```
If Riak is installed it is possible to run both unit tests and integration test. Futhermore, KV-specific integration tests are separated from TS-specific ones. To choose which set of tests to run appropriate maven profile should be selected: 

Profile name |Tests                                      | Default |
-------------|-------------------------------------------|---------|
riak_ts      | TS-specific tests and majority of KV-tests| no      |
riak_kv      | KV-only tests                             | yes     |
```
mvn clean verify -P riak_ts
mvn clean verify -P riak_kv
```
A Riak host can be provided in "com.basho.riak.pbchost" variable
```
mvn clean verify -P riak_ts -Dcom.basho.riak.pbchost=myhost:8087
```
If Riak was installed with devrel and is running on localhost on 10017 port, it is possible to use special "devrel" maven profile instead of providing "com.basho.riak.pbchost" variable
```
mvn clean verify -P devrel,riak_ts
```
Or
```
mvn clean verify -P riak_ts -Denvironment=devrel
```
Will do the same as 
```
mvn clean verify -P riak_ts -Dcom.basho.riak.pbchost=localhost:10017
```
