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
* Provides ability to construct an RDD by using a 2i string index or a set of indexes
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

If you're planning to develop Spark applications in Java there is an additional jar
spark-riak-connector-java-0.8.0.jar in spark-riak-connector-java/target/ 

Also connector depends on the following jars:
guava-14.0.1.jar
jackson-datatype-joda-2.2.2.jar
joda-time-2.1.jar
jackson-module-scala_2.10-2.4.4.jar
jcommon-1.0.23.jar
scala-java8-compat_2.10-0.3.0.jar
dataplatform-riak-client-2.0.2-SNAPSHOT.jar

All of these need to be referenced by your Spark application and accessible through driver program classpath

### Necessary imports

```scala
import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.rdd.RiakFunctions
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.spark.connector._
```

### Configuration

How to prepare `SparkContext` to connect to Riak

To connect your Spark application to Riak, the following options need to be set for `SparkConf` object.
These are prefixed with `spark.` so that they can be recognized
from the spark-shell and set within the $SPARK_HOME/conf/spark-default.conf.
The following options are available on `SparkConf` object:

Property name                                  | Description                                       | Default value
-----------------------------------------------|---------------------------------------------------|--------------------
spark.riak.connection.host                     | IP:port of a Riak node protobuf interface         | 127.0.0.1:8087
spark.riak.connection.host.connections.min     | Minimum number of parallel connections to Riak    | 10
spark.riak.connection.host.connections.max     | Maximum number of parallel connections to Riak    | 30


Example:

```scala
val conf = new SparkConf()
        .setAppName("My Spark Riak App")
        .set("spark.riak.connection.host", "10.0.4.1:8087")
        .set("spark.riak.connection.host.connections.min", "20")
        .set("spark.riak.connection.host.connections.max", "50")

val sc = new SparkContext("spark://10.0.4.1:7077", "test", conf)
```

### Reading data from Riak bucket

First we need to specify which Riak bucket (i.e. bucket named `test-data`)

```scala

val SOURCE_DATA = new Namespace("test-data")
```

Then we have several options to construct an RDD:

1. Query by an explicit set of keys

```scala

val rdd = sc.riakBucket(SOURCE_DATA).queryBucketKeys("key-1", "key-2", "key-2")
```

2. Query by a given 2i numeric range

```scala
val idxName = "myIndex"
val from = 0L
val to = 1000L

val rdd = sc.riakBucket(SOURCE_DATA).query2iRange(idxName, from, to)
```

   or a set of ranges

```scala
val idxName = "myIndex"

val rdd = sc.riakBucket(SOURCE_DATA).partitionBy2iRanges(idxName, 1->3, 4->6, 7->12)
```

3. Query by a given 2i string tag or set of tags

```scala

val rdd = sc.riakBucket(SOURCE_DATA).query2iKeys("mon_data", "wed_data", "fri_data")
```

### Doing something useful with the data

once the RDD is constructed all standard Scala Spark transformations and actions can be applied.
the simplest one would be to count all elements and print out the count

```scala

println(s"Element count: ${rdd.count()}")
```

### Saving results into Riak

To be able to write data out from an RDD into a Riak bucket the following import for a writer is needed:

```scala

import com.basho.spark.connector.writer.{WriteDataMapper, WriteDataMapperFactory}
```

define the output bucket and issue `saveToRiak` method on an RDD

```scala
val MY_OUTPUT_BUCKET = new Namespace("output-data")

rdd.saveToRiak(MY_OUTPUT_BUCKET)
```


An example of usage can be found in the [demo folder](https://github.com/basho/spark-riak-connector/tree/master/spark-riak-connector-demos)


