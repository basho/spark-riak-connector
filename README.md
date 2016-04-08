[![Build Status](https://travis-ci.org/basho/spark-riak-connector.svg?branch=develop)](https://travis-ci.org/basho/spark-riak-connector)

# Spark Riak Connector

Spark Riak connector allows you to expose data stored in Riak buckets as Spark RDDs, as well as output data from Spark RDDs into Riak buckets. 

## Compatibility
* Compatible with Riak KV
* Compatible with Riak TS
* Compatible with Apache Spark 1.5.2 or later
* Compatible with Scala 2.10 or later
* Compatible with Java 8


## Features
* Exposes data in Riak KV bucket as Spark RDD
* Provides ability to construct an RDD from a given set of keys
* Provides ability to construct an RDD using an enhanced 2i query (a.k.a. full bucket read) 
* Allows parallel full bucket reads into multiple partitions
* Allows saving of an RDD into a specified Riak bucket and indexing results with 2i indexes
* Provides mapping and data conversion for JSON formatted values
* Provides ability to construct an RDD by using a 2i string index or a set of indexes
* Provides ability to construct an RDD by using a 2i range query or a set of ranges 
* Provides ability to perform range queries over Riak TS bucket
* Provides ability to construct Dataframes from Riak TS bucket


## Building
Prerequisite: Java 8 JDK must be installed

If you don't have Maven yet, go to [Maven download page](https://maven.apache.org/download.cgi) and follow installation instructions for your OS.

Clone this repository, then build Spark Riak connector:

```
mvn clean install
```
To turn on streaming values support for PEx special maven profile "pex_streaming_vals" should be activated. It will make Full Bucket Reads more efficient: values will be streamed as a part of the FBR response instead of being fetched in a separate operations. This feature is supported only for Riak TS.
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
`spark-riak-connector/target/` contains `spark-riak-connector-1.0.0.jar` - this is the main connector jar. 


##Testing
In Spark Riak connector unit tests are separated from integration tests. 
In case if there is no Riak installation it is still possible to successfully run unit tests:
```
mvn clean test
```
If there is Riak installed it is possible to run both unit tests and integration test. Futhermore, KV-specific integration tests are separated from TS-specific ones. To choose which set of tests to run appropriete maven ptofile should be selected: 

Profile name |Tests                                      | Default |
-------------|-------------------------------------------|---------|
riak_ts      | TS-specific tests and majority of KV-tests| no      |
riak_kv      | KV-only tests                             | yes     |
```
mvn clean verify -P riak_ts
mvn clean verify -P riak_kv
```
Riak host can be provided in "com.basho.riak.pbchost" variable
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

## Developing 

If you're planning to develop Spark applications in Java there is an additional jar
`spark-riak-connector-java-1.0.0.jar` in `spark-riak-connector-java/target/`.

Connector depends on the following libraries:
* guava-14.0.1.jar
* jackson-datatype-joda-2.2.2.jar
* joda-time-2.1.jar
* jackson-module-scala_2.10-2.4.4.jar
* jcommon-1.0.23.jar
* scala-java8-compat_2.10-0.3.0.jar
* dataplatform-riak-client-1.0.0.jar

All of these need to be referenced by your Spark application and accessible through driver program classpath. 
Please see below code snippets in Scala, or explore the source code of bundled examples in Java and Scala.
 
The following link can be used to manual download [dataplatform-riak-client-1.0.0.jar](https://bintray.com/basho/data-platform/com.basho.riak/view)
from the Bintray repository.

To download dataplatform-riak-client.jar automatically during the build, the following repository should be added to pom.xml:

```xml
<repository>
    <id>bintray</id>
    <url>https://dl.bintray.com/basho/data-platform</url>
    <releases>
        <enabled>true</enabled>
    </releases>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
```

### Necessary imports

```scala
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.rdd.RiakFunctions
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.riak.spark._
```

### Configuration

You can prepare `SparkContext` to connect to Riak by connecting your Spark application to Riak. You will need to set the below options for `SparkConf` object. These options are prefixed with `spark.` so that they can be recognized
from the spark-shell and set within the $SPARK_HOME/conf/spark-default.conf.

The following options are available on `SparkConf` object:

Property name                                  | Description                                       | Default value      | Riak Type
-----------------------------------------------|---------------------------------------------------|--------------------|-------------
spark.riak.connection.host                     | IP:port of a Riak node protobuf interface         | 127.0.0.1:8087     | KV/TS
spark.riak.connections.min                     | Minimum number of parallel connections to Riak    | 10                 | KV/TS
spark.riak.connections.max                     | Maximum number of parallel connections to Riak    | 30                 | KV/TS
spark.riak.input.fetch-size                    | Number of keys to fetch in a single round-trip to Riak | 1000          | KV
spark.riak.input.split.count                   | Desired minimum number of Spark partitions to divide the data into | 10| KV
spark.riak.output.wquorum                      | Quorum value on write                                              | 1 | KV
spark.riak.connections.inactivity.timeout      | Time to keep connection to Riak alive in milliseconds | 1000 | KV/TS
spark.riakts.bindings.timestamp                | To treat/convert Riak TS timestamp columns either as a Long (UNIX milliseconds) or as a Timestamps during the automatic schema discovery. Valid values are: <ul><li>useLong</li><li>useTimestamp</li><ul> | useTimestamp | TS

Example:

```scala
val conf = new SparkConf()
        .setAppName("My Spark Riak App")
        .set("spark.riak.connection.host", "10.0.4.1:8087")
        .set("spark.riak.connections.min", "20")
        .set("spark.riak.connections.max", "50")

val sc = new SparkContext("spark://10.0.4.1:7077", "test", conf)
```

### Reading data from Riak bucket

First we need to specify which Riak bucket (i.e. bucket named `test-data`):

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

Once the RDD is constructed all standard Scala Spark transformations and actions can be applied.

The simplest action counts all elements and prints out the count:

```scala

println(s"Element count: ${rdd.count()}")
```

### Saving results into Riak KV

To be able to write data out from an RDD into a Riak bucket the following import for a writer is needed:

```scala

import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
```

Define the output bucket and issue `saveToRiak` method on an RDD:

```scala
val MY_OUTPUT_BUCKET = new Namespace("output-data")

rdd.saveToRiak(MY_OUTPUT_BUCKET)
```

### Reading data from Riak TS bucket

Riak TS buckets can be queried using sql() function:

```scala
val rdd = sc.riakTSTable(tableName).sql(s"SELECT * FROM $tableName WHERE time >= $from AND time <= $to")
```

### Saving rdd to Riak TS bucket

Existing rdd of org.apache.spark.sql.Row> can be saved to Riak TS as follows

```scala
rdd.saveToRiakTS(TABLE_NAME);
```

### Reading data from Riak TS into Dataframe
To read data from existing TS table *tableName* standard SQLContext means can be utilized with special **org.apache.spark.sql.riak** data format and using range query expression.
For example, 
```scala
val schema = StructType(List(
    StructField(name = "col1", dataType = StringType),
    StructField(name = "col2", dataType = StringType),
    StructField(name = "time", dataType = TimestampType),
    ...
)
...
val df = sqlContext.read   
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(tableName)
      .filter(s"time >= CAST($from AS TIMESTAMP) AND time <= CAST($to AS TIMESTAMP)")
```
Providing schema is optional. If it's not provided it will be inferred using schema discovery to return bucket structure from RiakTS.
Any of the Spark Connector options can be provided in .option() or .options():
```scala
val df = sqlContext.read
      .option("spark.riakts.bindings.timestamp", "useLong")    
      .format("org.apache.spark.sql.riak")
      ...
```

### Range query partitioning for RiakTS
RiakTS is known to have limitation on range query: *time range must not exceed 5 quanta*. In order to get around this limitation or simply achieve higher read performance large ranges can be split into smaller subranges at partitioning time.
To use this functionality it's required to provide the following options:
* **spark.riak.partitioning.ts-range-field-name** to identify quantized field
* **spark.riak.input.split.count** to identify number of partitions/subranges (default value is **10**)

For example,
```scala
   val df = sqlContext.read
      .option("spark.riak.input.split.count", "5")
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(tableName)
      .filter(s"time >= CAST(111111 AS TIMESTAMP) AND time <= CAST(555555 AS TIMESTAMP) AND col1 = 'val1'")
```
Initial range query will be split into 5 subqueries (one per each partition) as follows:
* ```time >= CAST(111111 AS TIMESTAMP) AND time < CAST(222222 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(222222 AS TIMESTAMP) AND time < CAST(333333 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(333333 AS TIMESTAMP) AND time < CAST(444444 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(444444 AS TIMESTAMP) AND time < CAST(555555 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(555555 AS TIMESTAMP) AND time < CAST(555556 AS TIMESTAMP) AND col1 = 'val1'```

Not providing **spark.riak.partitioning.ts-range-field-name** property results into having single partition with initial query.

### Save Dataframe to Riak TS
Existing inputDF that has the same schema as TS bucket (column order and types) can be saved to Riak TS as follows: 
```scala
inputDF.write
   .option("spark.riak.connection.hosts","myhost:10017")
   .format("org.apache.spark.sql.riak")
   .mode(SaveMode.Append)
   .save(tableName)
```
So far *SaveMode.Append* is the only mode available.

## Examples

Riak Spark connector comes with several sample programs and demos that can be found in the [**examples** folder](./examples)


