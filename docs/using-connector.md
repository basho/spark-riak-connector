# Using The Spark-Riak Connector

This document will walk you through setting up your application for development with the Spark-Riak connector.

**Note: Currently, if you are using Python, only Riak TS tables, Spark DataFrames and Spark SQL are supported. Reading and writing to Riak KV buckets is not supported yet with Python.**

Scroll down or click below for the desired information:
- [Configuration of Spark Context](./using-connector.md#configuration-of-spark-context)
- [Reading Data From KV Bucket](./using-connector.md#reading-data-from-kv-bucket)
- [Writing Data To KV Bucket](./using-connector.md#writing-data-to-kv-bucket)
- [Writing Data To KV Bucket With 2i Indices](./using-connector.md#writing-data-to-kv-bucket-with-2i-indices)
- [Reading Data From TS Table](./using-connector.md#reading-data-from-ts-table)
- [Writing Data To TS Table](./using-connector.md#writing-data-to-ts-table)
- [Spark Dataframes With KV Bucket](./using-connector.md#spark-dataframes-with-kv-bucket)
- [Spark Dataframes With TS Table](./using-connector.md#spark-dataframes-with-ts-table)
- [Partitioning for KV Buckets](./using-connector.md#partitioning-for-kv-buckets)
- [Working With TS Dates](./using-connector.md#working-with-ts-dates)
- [TS Table Range Query Partitioning](./using-connector.md#ts-table-range-query-partitioning)
- [TS Bulk Write](./using-connector.md#ts-bulk-write)
- [Spark Streaming Example](./using-connector.md#spark-streaming-example)
- [Using Java With The Connector](./using-connector.md#using-java-with-the-connector)



## Configuration of Spark Context

The following import statements should be included at the top of your Spark application to enable the connector:

**Scala**
```scala
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.rdd.RiakFunctions
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.riak.spark._
```
**Python**
```python
import pyspark
```
You can control how your Spark application interacts with Riak by configuring different options for your `SparkContext` or `SQLContext`. You can set these options within the $SPARK_HOME/conf/spark-default.conf.  If you don't set an option, it will be automatically set to the default values listed below.

You can set the below options for the `SparkConf` object:

Property name                                  | Description                                       | Default value      | Riak Type
-----------------------------------------------|---------------------------------------------------|--------------------|-------------
spark.riak.connection.host                     | IP:port of a Riak node protobuf interface         | 127.0.0.1:8087     | KV/TS
spark.riak.connections.min                     | Minimum number of parallel connections to Riak    | 20                 | KV/TS
spark.riak.connections.max                     | Maximum number of parallel connections to Riak    | 30                 | KV/TS
spark.riak.input.fetch-size                    | Number of keys to fetch in a single round-trip to Riak | 1000          | KV
spark.riak.input.split.count                   | Desired minimum number of Spark partitions to divide the data into | 10| KV
spark.riak.write.replicas                      | Quorum value on write. Integer value or symbolic constant can be used. Possible symbolic constants are: <ul><li>all - All replicas must reply.</li><li>one - This is the same as integer value 1.</li><li>quorum - A majority of the replicas must respond, that is, “half plus one”.</li><li>default - Uses whatever the per-bucket consistency property, which may be any of the above values, or an integer.</li></ul>                                              | default | KV
spark.riak.connections.inactivity.timeout      | Time to keep connection to Riak alive in milliseconds | 1000 | KV/TS
spark.riakts.bindings.timestamp                | To treat/convert Riak TS timestamp columns either as a Long (UNIX milliseconds) or as a Timestamps during the automatic schema discovery. Valid values are: <ul><li>useLong</li><li>useTimestamp</li><ul> | useTimestamp | TS
spark.riak.partitioning.ts-range-field-name    | Name of quantized field for range query       | 1                  | TS
spark.riakts.write.bulk-size                   | Bulk size for parallel TS table writes            | 100                | TS

Example:

**Scala**
```scala
val conf = new SparkConf()
        .setAppName("My Spark Riak App")
        .set("spark.riak.connection.host", "127.0.0.1:8087")
        .set("spark.riak.connections.min", "20")
        .set("spark.riak.connections.max", "50")

val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
```

**Python**
```python
conf = pyspark.SparkConf().setAppName("My Spark Riak App")
conf.set("spark.riak.connection.host", "127.0.0.1:8087")
conf.set("spark.riak.connections.min", "20")
conf.set("spark.riak.connections.max", "50")
sc = pyspark.SparkContext("spark://127.0.0.1:7077", "test", conf)
```

## Reading Data From KV Bucket

Once a SparkContext is created, we can load data stored in Riak KV buckets into Spark as RDDs. To specify which bucket to use:

```scala
val kv_bucket_name = new Namespace("test-data")
```

Let's do a simple but very powerful full bucket read. We're going to read the content of entire Riak KV bucket in one command, and it will happen in an efficient partitioned parallel way and get values as Strings:

```scala
 val data = sc.riakBucket[String](kv_bucket_name).queryAll()
```

When you know your keys by name, you can pass them in directly:

```scala
val rdd = sc.riakBucket[String](kv_bucket_name).queryBucketKeys("Alice", "Bob", "Charlie")
```

You can also specifiy a range of values (say, from 1 to 5000) defined by a numeric 2i index in Riak if your index is `myIndex`:

```scala
val rdd = sc.riakBucket[String](kv_bucket_name).query2iRange("myIndex", 1L, 5000L)
```

You can also specify a set of numeric 2i range values to query by:

```scala
val rdd = sc.riakBucket[String](kv_bucket_name).partitionBy2iRanges("myIndex", 1->3, 4->6, 7->12)
```

You can also query by a 2i string tag or set of 2i string tags:

```scala
val rdd = sc.riakBucket[String](kv_bucket_name).query2iKeys("mon_data", "wed_data", "fri_data")
```

## Writing Data To KV Bucket

To be able to write data out from an RDD into a Riak KV bucket, the following import for a writer is needed:

```scala
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
```

Define the output bucket and issue `saveToRiak` method on an RDD:

```scala
val output_kv_bucket = new Namespace("test-bucket")
rdd.saveToRiak(output_kv_bucket)
```

## Writing Data To KV Bucket With 2i Indices

There are two ways to add 2i indices to your data when writing into KV buckets. The first way involves creating a `case class` for your data and annotating the key and 2i index fields. In the following example, we create a data object called `ORMDomainObject`, annotate the key field with `@(RiakKey@field)`, and annotate the 2i field with `@(RiakIndex@field) (name = "groupId")`. Then we create a list of data, create an RDD with `val rdd:RDD[ORMDomainObject] = sc.parallelize(data, 1)` and write to a KV bucket with `rdd.saveToRiak(bucket)`. Note that `login: String` is just a regular data field, it is not a key or a 2i index.

```scala

case class ORMDomainObject(
    @(RiakKey@field)
    user_id: String,

    @(RiakIndex@field) (name = "groupId")
    group_id: Long,

    login: String)
  
  
    val data = List(
    ORMDomainObject("u1", 100, "user 1"), 
    ORMDomainObject("u2", 200, "user 2"), 
    ORMDomainObject("u3", 100, "user 3")
    )
    val rdd:RDD[ORMDomainObject] = sc.parallelize(data, 1)

    rdd.saveToRiak(bucket)
```

The second way of writing data with 2i indices to KV bucket is slightly more complicated. You must create a `case class` for a user defined data type that describes the data. Then, place some instances of the data into an RDD. You will then need to modify the custom data mapper section. The data mapper wil take in the user defined data type and map a key-value pair with 2i indices to RiakObjects. If the user defined data type is called something other than `DomainObject` you will need to change these parameters to be the name of your user defined data type. Then you will need to supply what type the 2i index is in the following:

```scala
ro.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named("groupId")).add(value.group_id)
```

Here we are getting all `LongIntIndex` 2i indices and adding `group_id` to that list. If you wanted something other than a numeric 2i index, you could change `LongIntIndex` to `StringIndex`. Next, we are setting the return value of the mapper to `(value.user_id, ro)` which is a key-value pair that represents the structure of our RiakObjects. Finally, we store our RDD into a KV bucket.

```scala
case class DomainObject(
    user_id: String,
    group_id: Long,
    login: String)
  
  
  val data = List(
    DomainObject("u1", 100, "user 1"), 
    DomainObject("u2", 200,"user 2"), 
    DomainObject("u3", 100, "user 3")
    )
    val rdd:RDD[DomainObject] = sc.parallelize(data, 1)
    
  // create custom data mapper
    implicit val vwf = new WriteDataMapperFactory[DomainObject, KeyValue] {
      override def dataMapper(bucket: BucketDef): WriteDataMapper[DomainObject, KeyValue] = {
        new WriteDataMapper[DomainObject, KeyValue] {
      override def mapValue(value: DomainObject): (String, RiakObject) = {
        val ro = RiakObjectConversionUtil.to(value)

        ro.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named("groupId")).add(value.group_id)

        (value.user_id, ro)
          }
        }
      }
    }

    rdd.saveToRiak(bucket)
```

## Reading Data From TS Table

Riak TS tables can be queried using the `sql()` method:

```scala
val ts_table_name = "test-table"
val rdd = sc.riakTSTable(ts_table_name)
			.sql(s"SELECT * FROM $ts_table_name WHERE time >= $from AND time <= $to")
```

## Writing Data To TS Table

You can save an RDD of type `<org.apache.spark.sql.Row>` to Riak TS as follows

```scala
val output_ts_table = "test-bucket"
rdd.saveToRiakTS(output_ts_table);
```

## Spark Dataframes With KV Bucket

You can use Spark DataFrames on top of an RDD that was created from a KV Bucket. First you need to create a SQLContext from SparkContext:

```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
```

Then import:

```scala    
import sqlContext.implicits._
```

Next, you have to specify a user defined type to allow schema inference using reflection:

```scala
case class UserData(user_id: String, name: String, age: Int, category: String)
```    

Then, you can use the toDF() method on your RDD.

```scala  
val kv_bucket_name = new Namespace("test-data")
val riakRdd = sc.riakBucket[UserData](kv_bucket_name).queryAll()
val df = riakRdd.toDF()
```

Once you have your DataFrame you can use its methods for filtering

```scala
df.where(df("age") >= 50).select("id", "name")
```

or do more complex operations like grouping.


```scala
df.groupBy("category").count
```

Alternatively, you can register a table

```scala
df.registerTempTable("users")
```

and use Spark SQL queries over it.


```scala
sqlContext.sql("select * from users where age >= 50")
```

Another thing you can use are user defined functions (UDFs). First, you have to register a UDF.

```scala
sqlContext.udf.register("stringLength", (s: String) => s.length)
```

After that you can use it in SQL queries   

```scala
sqlContext.sql("select user_id, name, stringLength(name) nameLength from users order by nameLength")
```
When you already have a DataFrame, you can save it into Riak. To do that, make sure you have imported `com.basho.riak.spark._` so that saveToRiak() method is available.

```scala
import com.basho.riak.spark._
```

Then you can use toJSON() method to save data to riak in json format

```
dataFrame.toJSON.map {
	line =>
		val obj = RiakObjectConversionUtil.to(line)
		obj.setContentType("application/json")    
		obj
	}.saveToRiak(namespace)
```

Setting content type to `application/json` will allow automatic conversion to user defined type later when reading from Riak.

## Spark Dataframes With TS Table

To enable DataFrames functionality, first steps are 

**Scala**
```scala
val sc = new SparkContext()
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
ts_table_name = "test_table"
```
**Python**
```python
sc = pyspark.SparkContext(conf=conf)
sqlContext = pyspark.SQLContext(sc)
ts_table_name = "test_table"
```

To read data from existing TS table `test-table` standard SQLContext means can be used by providing a special `“org.apache.spark.sql.riak”` data format and using a Riak TS range query: 

**Scala**
```scala
val df = sqlContext.read   
 	.option("spark.riak.connection.hosts","riak_host_ip:10017")
  	.format("org.apache.spark.sql.riak")
  	.load(ts_table_name)
	.select(“time”, “col1”, “col2”)
  	.filter(s"time >= CAST($from AS TIMESTAMP) AND time <= CAST($to AS TIMESTAMP) AND  col1= $value1")
```
**Python**
```python
df = sqlContext.read \
	.option("spark.riak.connection.hosts","riak_host_ip:10017") \
  	.format("org.apache.spark.sql.riak") \
  	.load(ts_table_name) \
	.select(“time”, “col1”, “col2”) \
  	.filter(s"time >= CAST($from AS TIMESTAMP) AND time <= CAST($to AS TIMESTAMP) AND  col1= $value1")
```

Schema may or may not be provided using `.schema()` method. If not provided, it will be inferred. Any of the Spark Connector options can be provided in `.option()` or `.options()`. Alternatively, `org.apache.spark.sql.riak.RiakSQLContext` can be created and then queried with range query using `sql()` method

**Scala**
```scala
val riakSqlContext = new RiakSQLContext(sc, ts_table_name)
val alternativeDf = riakSqlContext.sql(s"SELECT time, col1 from $ts_table_name WHERE time >= CAST($from AS TIMESTAMP) AND time <= CAST($to AS TIMESTAMP) AND  col1= $value1")
```

A DataFrame, `inputDF`, that has the same schema as an existing TS table (column order and types) can be saved to Riak TS as follows: 

**Scala**
```scala
inputDF.write
   .option("spark.riak.connection.hosts","riak_host_ip:10017")
   .format("org.apache.spark.sql.riak")
   .mode(SaveMode.Append)
   .save(ts_table_name)
```
**Python**
```python
inputDF.write \
   .option("spark.riak.connection.hosts","riak_host_ip:10017") \
   .format("org.apache.spark.sql.riak") \
   .mode(SaveMode.Append) \
   .save(ts_table_name)
```

So far SaveMode.Append is the only mode available.
Any of the Spark Connector options can be provided in `.option()` or `.options()`.


## Partitioning for KV Buckets

### Key Based Partitioning
Querying with the following methods with result in a RDD with single partition:

* query2iRange(index, from, to)
* query2iKeys(index, keys*)
* queryBucketKeys(keys*)

The following methods will split the RDD into multiple partitions:
* partitionBy2iRanges(index, ranges*) will create a partition for each of the  input ranges

```scala
val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .partitionBy2iRanges(CREATION_INDEX, 1 -> 3, 4 -> 6, 7 -> 12)
```

* partitionBy2iKeys(index: String, keys*) will create a partition for each of the input keys

```scala
    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .partitionBy2iKeys("category", "neighbor", "visitor", "stranger")
```

### Coverage Plan Based Partitioning

A coverage plan is Riak's description of what keys are stored on what nodes of the cluster.
The coverage plan based partitioner will be used for the following queries:
* queryAll()
* query2iRangeLocal(index, from, to)

First, a query for the coverage plan is made to Riak. Then, the returned coverage entries (one for each VNode) are grouped by host and split into a number of partitions (defined by **spark.riak.input.split.count**) in such a way that each partition reads data from a single host. This means that each partition can processes multiple coverage entries but all of the parition will point to single Riak node (if it's possioble). While processing, the coverage entry partition will iteratively read data in portions. The size of a portion is defined by **spark.riak.input.fetch-size**.

## Working With TS Dates

Riak TS automactically stores all datetimes as a Long integer that represents milliseconds from the beginning of the epic. This is not very human friendly so we have provided a Spark configuration option called `spark.riakts.bindings.timestamp`. This option is for use with Automatic Schema Discovery and allows for conversion from Riak TS datetimes, which are stored as Longs, to Timestamps.  The default value of this option is `useTimestamp` which converts Longs to Timestamps. If you would like to use the original Long value, you can use the option value of `useLong`. All conversion takes place during Automatic Schema Discovery when reading from Riak TS tables.

You can provide the schema manually:

```scala
val schemaWithLong = StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val df = sqlContext.read
      .format("org.apache.spark.sql.riak")
      .schema(newSchema)
      .load(tableName)
      .filter(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'")
```

You can use `spark.riakts.bindings.timestamp` and Automatic Schema Discovery with `useLong`:

```scala
val df = sqlContext.read
      .format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useLong")
      .load(tableName)
      .filter(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
```
In the previous example, the query times, `queryFromMillis` and `queryToMillis`, are Long integers since the datetime values in `df` are stored as Long integers.

Or, you can use `spark.riakts.bindings.timestamp` and Automatic Schema Discovery with `useTimestamp`:

```scala
val df = sqlContext.read
      .format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useTimestamp")
      .load(tableName)
      .filter(s"time > CAST('$from' AS TIMESTAMP) AND time < CAST('$to' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")
```
In the previous example, the query times, `CAST('$from' AS TIMESTAMP)` and `CAST('$to' AS TIMESTAMP)`, are Timestamps which are cast from a Long integer since the datetime values in `df` are stored as Timestamps.


## TS Table Range Query Partitioning

Riak TS range queries are limited to a maximum of 5 quanta (see http://docs.basho.com/riakts/latest/using/querying/). To work around this limitation or simply achieve higher read performance, large ranges can be split into smaller sub-ranges at partitioning time.

To use this functionality it's required to provide the following options:
* `spark.riak.partitioning.ts-range-field-name` to identify quantized field
* `spark.riak.input.split.count` to identify number of partitions/subranges (default value is `10`)

For example:

**Scala**
```scala
   val df = sqlContext.read
      .option("spark.riak.input.split.count", "5")
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(ts_table_name)
      .filter(s"time >= CAST(111111 AS TIMESTAMP) AND time <= CAST(555555 AS TIMESTAMP) AND col1 = 'val1'")
```
**Python**
```python
df = sqlContext.read \
      .option("spark.riak.input.split.count", "5") \
      .option("spark.riak.partitioning.ts-range-field-name", "time") \
      .format("org.apache.spark.sql.riak") \
      .schema(schema) \
      .load(ts_table_name) \
      .filter(s"time >= CAST(111111 AS TIMESTAMP) AND time <= CAST(555555 AS TIMESTAMP) AND col1 = 'val1'")

```

The initial range query will be split into 5 subqueries (one per each partition) as follows:
* ```time >= CAST(111111 AS TIMESTAMP) AND time < CAST(222222 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(222222 AS TIMESTAMP) AND time < CAST(333333 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(333333 AS TIMESTAMP) AND time < CAST(444444 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(444444 AS TIMESTAMP) AND time < CAST(555555 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(555555 AS TIMESTAMP) AND time < CAST(555556 AS TIMESTAMP) AND col1 = 'val1'```

Not providing the `spark.riak.partitioning.ts-range-field-name` property will default to having a single partition with initial query.

## TS Bulk Write

To write into a TS table, the Spark-Riak Connector splits the intial set of rows into smaller bulks and processes them in parallel. Bulk size can be configured using `spark.riakts.write.bulk-size` property. The default number is `100`. As an example, lets say your RDD has 2000 rows and you set `spark.riakts.write.bulk-size` to 200 and `spark.riak.connections.min` to 5. Then, there will be 10 bulks with 200 rows and each bulk will have 5 parallel write connections to Riak. The bulk size option can be configured in SparkConf:

**Scala**
```scala
val conf = new SparkConf().set("spark.riakts.write.bulk-size", "500")
```
**Python**
```python
conf = new SparkConf().set("spark.riakts.write.bulk-size", "500")
```

Or you can set the `spark.riakts.write.bulk-size` property in the DataFrame's `.option()`:

**Scala**
```scala
val df = sqlContext.write
	.option("spark.riakts.write.bulk-size", "500")
      	.format("org.apache.spark.sql.riak")
      	.mode(SaveMode.Append)
      	.save(bucketName)
```
**Python**
```python
df = sqlContext.write
	.option("spark.riakts.write.bulk-size", "500")
      	.format("org.apache.spark.sql.riak")
      	.mode(SaveMode.Append)
      	.save(bucketName)
```

Bulks will be written in parallel. The number of parallel writes for each partition is defined with the `spark.riak.connections.min` property (default is `20`):

**Scala**
```scala
val conf = new SparkConf()
	.set("spark.riakts.write.bulk-size", "500")
        .set("spark.riak.connections.min", "50")
```
**Python**
```python
conf = pyspark.SparkConf()
conf.set("spark.riakts.write.bulk-size", "500")
conf.set("spark.riak.connections.min", "50")
```

## Spark Streaming Example

The Spark-Riak Connector can be used with Spark Streaming. To demonstrate this usage, we will work through two small Scala examples. These examples are located in the [examples folder](../examples/src/main/scala/com/basho/riak/spark/examples) of the Spark-Riak Connector repo.

These examples require the use of Kafka. Please install Kafka and setup a Kafka broker prior to running this example. We will assume that there is a Kafka broker running at `127.0.0.1:9092` with a topic called `streaming`. Instructions on setting up kafka topics can be found in [this guide](https://kafka.apache.org/documentation.html#quickstart). We also assume Riak TS is installed and there is a Riak TS node running at `127.0.0.1:8087`. You will need to build the connector as well. Please follow the instruction on [building the connector](./building-and-testing-connector.md#build). After setting up, there are two examples to run: a KV bucket example and a TS table example.

###Spark Streaming KV Buckets Example

Now that we are set up, lets look at the KV bucket example [here](../examples/src/main/scala/com/basho/riak/spark/examples/streaming/StreamingKVExample.scala). 

In the first chunck of code in the main method, we are just setting up our local spark streaming context and setting the name for the KV bucket to `test-data`:

```scala
val sparkConf = new SparkConf(true)
      .setAppName("Simple Spark Streaming to Riak KV Demo")
setSparkOpt(sparkConf, "spark.master", "local")
setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")

val sc = new SparkContext(sparkConf)
val streamCtx = new StreamingContext(sc, Durations.seconds(15))
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
val namespace = new Namespace("test-data")
```

Next we are setting up kafka properties:

```scala
val kafkaProps = Map[String, String](
      "metadata.broker.list" -> "127.0.0.1:9092",
      "client.id" -> UUID.randomUUID().toString
    )
```

Then, we are using `KafkaUtils` to create a stream from the kafka topic `streaming` into our KV bucket `test-data`:

```scala
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](streamCtx, kafkaProps, Set[String]("streaming"))
      .foreachRDD { rdd =>
        val rows = sqlContext.read.json(rdd.values).map {
          line => val obj = RiakObjectConversionUtil.to(line)
            obj.setContentType("application/json")
            obj
        }.saveToRiak(namespace)
      }
```
 And finally, we are starting the stream:
 
 ```scala
 streamCtx.start()
 streamCtx.awaitTermination()
 ```

 Now that we understand the code, we can run the `StreamingKVExample.scala` example with:
 
 ```
 /path/to/spark-riak-connector-examples/bin/run-example streaming.StreamingKVExample
 ```
 
 This wil start a stream from the Kafka topic `streaming` into the KV bucket `test-data` that we just created. This stream will run until terminated. Whenever a message is produced for Kafka topic `streaming`, the spark streaming context that the example creates will automatically stream the message from the topic into the TS table. To see this in action, we need to send a message to the kafka topic `streaming` with:
 
 ```
 /path/to/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streaming
 ```
 
 This will read a message from standard in and pass it to the topic. As an example put the following into standard in:
 
 ```
 {"time": "2016-01-01 08:30:00.000", "weather": "sunny", "temperature": 25.0, "humidity": 67.0, "pressure": 30.20, "family": "f"}
 ```
 
You should now be able to see this data entry in the KV bucket `test-data`.

###Spark Streaming TS Table Example

Having seen how spark streaming works with KV buckets, lets now look at the TS table example [here](../examples/src/main/scala/com/basho/riak/spark/examples/streaming/StreamingTSExample.scala). 

The code is somewhat similar to the KV bucket example, but with crucial differences. Let have a look:

```scala
 val schema = StructType(List(
      StructField(name = "weather", dataType = StringType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = TimestampType),
      StructField(name = "temperature", dataType = DoubleType),
      StructField(name = "humidity", dataType = DoubleType),
      StructField(name = "pressure", dataType = DoubleType)))

    val sparkConf = new SparkConf(true)
      .setAppName("Simple Spark Streaming to Riak TS Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    setSparkOpt(sparkConf, "kafka.broker", "127.0.0.1:9092")

    val sc = new SparkContext(sparkConf)
    val streamCtx = new StreamingContext(sc, Durations.seconds(15))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val kafkaProps = Map[String, String](
      "metadata.broker.list" -> sparkConf.get("kafka.broker"),
      "client.id" -> UUID.randomUUID().toString
    )
```

This first section of code just sets up the table schema, a spark streaming context, a spark sql context, and Kafka properties. Note that we need to set up a table in TS that reflect the schema in the previous section of code. We can create this table in TS with:

```
curl -v -XPUT -H 'Content-Type: application/json' "http://$RIAK_HTTP/admin/explore/clusters/default/bucket_types/ts_weather_demo" -d '{"props":{"n_val":3, "table_def":"CREATE TABLE ts_weather_demo (weather varchar not null,family varchar not null,time timestamp not null,temperature  double,humidity double,pressure double,PRIMARY KEY ((weather, family, quantum(time, 1, 'h')), weather, family, time))"}}'
```

Be sure to substitute the Riak's IP address and http port in for `$RIAK_HTTP`.

The next section of the code is:

```scala
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](streamCtx, kafkaProps,
      Set[String]("streaming"))
      .foreachRDD { rdd => rdd.map(println)
        val rows = sqlContext.read.schema(schema).json(rdd.values)
          .withColumn("time", 'time.cast("Timestamp"))
          .select("weather", "family", "time", "temperature", "humidity", "pressure")

        rows.write
          .format("org.apache.spark.sql.riak")
          .mode(SaveMode.Append)
          .save("ts_weather_demo")
      }
```
In this section of code, we are setting up a stream from Kafak topic `streaming` into TS table `ts_weather_demo`. Here we are using our spark sql context to read each rdd streamed from the kafka topic and then write into the TS table.

Now we need to actually send data to the Kafka topic. Lets start `kafka-console-producer.sh` and send a chunk of data to it with:

 ```
 /path/to/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streaming
 ```
 
Then the data:

 ```
 {"time": "2016-01-01 08:30:00.000", "weather": "sunny", "temperature": 25.0, "humidity": 67.0, "pressure": 30.20, "family": "f"}
 ```

You can check that this worked by doing a simple SQL for the example data. 

## Using Java With The Connector

To use Riak KV and Riak TS features of Spark Riak connector in Java applications first thing that should be done is to create javaSparkContext:

```java
JavaSparkContext jsc = new JavaSparkContext(sparkConf);
```

Further steps are different for KV Buckets and TS Tables.

### Read From KV Bucket

To use any of the Riak query functions, initial RiakJavaRDD must be created by using any of the SparkContextJavaFunctions.riakBucket(...) methods. The resulting RiakJavaRDD still needs query criteria to perform the following operations:

Load all data from a bucket - returns all existing data from the bucket:

```java
SparkJavaUtil.javaFunctions(jsc).riakBucket(NAMESPACE, String.class).queryAll();
```

Load data for a range of index values - returns only data that have index value inside a range (inclusive):

```java
SparkJavaUtil.javaFunctions(jsc).riakBucket(NAMESPACE, String.class).query2iRangeLocal(INDEX_NAME, from, to);
```

Load data for a list of index values - returns only data that have index value inside a list:

```java
SparkJavaUtil.javaFunctions(jsc).riakBucket(NAMESPACE, String.class).query2iKeys(INDEX_NAME, iValue1, iValue2, ...);
```
    
Load data by keys - return only data for listed keys:

```java
SparkJavaUtil.javaFunctions(jsc).riakBucket(NAMESPACE, String.class).queryBucketKeys("key-1", "key-3", "key-6", ...)
```

### Write To KV Bucket

An existing JavaRDD<{UserDefinedType}>, `rdd`, can be saved to KV bucket as follows 

```java
SparkJavaUtil.javaFunctions(rdd).saveToRiak(NAMESPACE);
```

{UserDefinedType} must be serializable and can use annotations from com.basho.riak.client.api.annotations package.

```java
class SimpleUDT implements Serializable {
	@RiakIndex(name = "creationNo")
	private long creationNo;
	private String value;
	@RiakKey
	private String key;
...
}
```

### Read From TS Table

To use any of the Riak query functions, the initial RiakTSJavaRDD must be created by using SparkContextJavaFunctions.riakTSBucket() method. The resulting RiakTSJavaRDD still needs a sql query string to perform range scan:

```java

String test_query = "SELECT * FROM %s WHERE time >= %d AND time <= %d  AND  weather = 'sunny'"

SparkJavaUtil.javaFunctions(jsc)
	.riakTSTable(TABLE_NAME, Row.class)
	.sql(String.format(test_query, TABLE_NAME, from, to));
```

### Write To TS Table

An existing JavaRDD<org.apache.spark.sql.Row>, `sqlRdd`, can be saved to Riak TS as follows

```java
SparkJavaUtil.javaFunctions(sqlRdd).saveToRiakTS(TABLE_NAME);
```
