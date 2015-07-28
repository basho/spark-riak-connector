## Spark Riak Connector

Spark Riak Connector allows you to expose the data stored in Riak buckets as Spark RDDs as well as to output data from Spark RDDs into Riak buckets. 

### Compatibility
* Compatible with Riak version 2.0 or higher
* Compatible with Apache Spark 1.4 or higher
* Compatible with Scala 2.10
* Compatible with Java 8


### Features
* Exposes data in Riak bucket as Spark RDD
* Provides ability to construct an RDD form a given set of keys
* Provides ability to construct an RDD by using a 2i stirng index or a set of indexes
* Provides ability to construct an RDD by using a 2i range query or a set of ranges
* Provides ability to construct an RDD using a full bucket read 
* Allows saving of an RDD into a specified Riak bucket and indexing results with 2i indexes
* Provides mapping and data conversion for JSON formatted values
* Allows parallel ful bucket reads into multiple partitions


### Building
Prerequisite: Java 8 JDK must be installed

If you don't have Maven yet, go to [Maven download page](https://maven.apache.org/download.cgi) and follow installation instructions for your OS.

Clone this repository, then build the Spark Riak Connector:

```
mvn clean install
```

Once connector is built there are several jars that are produced:
spark-riak-connector/target/ contains spark-riak-connector-0.8.0.jar - this is the main connector jar. 

if you're planning to develop Spark applications in Java there is an additional jar
spark-riak-connector-java-0.8.0.jar in spark-riak-connector-java/target/ 

Also conector requires the following jars:
guava-14.0.1.jar
jackson-datatype-joda-2.2.2.jar
joda-time-2.1.jar
jackson-module-scala_2.10-2.4.4.jar
jcommon-1.0.23.jar
scala-java8-compat_2.10-0.3.0.jar

All of these need to be referenced by your Spark application and accessible through driver program classpath

An example of usage can be found in the [demo folder](https://github.com/basho/spark-riak-connector/tree/master/spark-riak-connector-demos)


