# Using The Spark-Riak Connector

This document will walk you through setting up your application for development with the Spark-Riak connector.

Scroll down or click below for the desired information:
- [Configuration of Spark Context](./using-connector.md#configuration-of-spark-context)
- [Failover Handling](./using-connector.md#failover-handling)
- [Reading Data From KV Bucket](./using-connector.md#reading-data-from-kv-bucket)
- [Writing Data To KV Bucket](./using-connector.md#writing-data-to-kv-bucket)
- [Writing Data To KV Bucket With 2i Indices](./using-connector.md#writing-data-to-kv-bucket-with-2i-indices)
- [Reading Data From TS Table](./using-connector.md#reading-data-from-ts-table)
- [Writing Data To TS Table](./using-connector.md#writing-data-to-ts-table)
- [Spark Dataframes With KV Bucket](./using-connector.md#spark-dataframes-with-kv-bucket)
- [Spark Dataframes With TS Table](./using-connector.md#spark-dataframes-with-ts-table)
- [Spark DataSets With TS Table](./using-connector.md#spark-datasets-with-ts-table)
- [Partitioning for KV Buckets](./using-connector.md#partitioning-for-kv-buckets)
- [Working With TS Dates](./using-connector.md#working-with-ts-dates)
- [Partitioning for Riak TS Table Queries](./using-connector.md#partitioning-for-riak-ts-table-queries)
- [TS Bulk Write](./using-connector.md#ts-bulk-write)
- [Using Jupyter Notebook](./using-connector.md#using-jupyter-notebook)
- [Spark Streaming](./using-connector.md#spark-streaming)
- [Using Java With The Connector](./using-connector.md#using-java-with-the-connector)



## Configuration of Spark Context

The following import statements should be included at the top of your Spark application to enable the connector:

**Scala**
```scala
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import com.basho.riak.spark._
```
**Python**
```python
import pyspark
import pyspark_riak
import pyspark_riak.sql
```
You can control how your Spark application interacts with Riak by configuring different options for your `SparkSession`. You can set these options within the $SPARK_HOME/conf/spark-default.conf.  If you don't set an option, it will be automatically set to the default values listed below.

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
spark.riak.partitioning.ts-quantum             | Size of the quantum for range field E.g.: "100s"  | N/A                | TS

Example:

**Scala**
```scala
val conf = new SparkConf()
        .setAppName("My Spark Riak App")
        .set("spark.riak.connection.host", "127.0.0.1:8087")
        .set("spark.riak.connections.min", "20")
        .set("spark.riak.connections.max", "50")
val sparkSession = SparkSession.builder()
                        .master("spark://127.0.0.1:7077")
                        .appName("test")
                        .config(conf)
                        .getOrCreate()
val sc = sparkSession.sparkContext
```

**Python**
```python
conf = pyspark.SparkConf().setAppName("My Spark Riak App")
conf.set("spark.riak.connection.host", "127.0.0.1:8087")
conf.set("spark.riak.connections.min", "20")
conf.set("spark.riak.connections.max", "50")
sc = pyspark.SparkContext("spark://127.0.0.1:7077", "test", conf)

pyspark_riak.riak_context(sc)
```


## Failover Handling

When reading data from a Riak KV bucket, the Spark-Riak connector will run a full bucket query. This query first obtains metadata, called a coverage plan, from Riak about
where the data is located in the cluster. This coverage plan will have a list of nodes were the data lives. After the Spark-Riak connector recieves the coverage plan, a node may fail and the reads going to the node will also
fail. In this event, the Spark-Riak connector will request and alternative coverage plan from Riak for the data that failed to be read from Riak. This alternative
coverage plan will return the next node that holds the data that we are attempting to read. Then the Spark-Riak connector will attempt to read the missing data
from the nodes in the alternative coverage plan. This will continue until the missing data is successfully read or the list of alternative nodes has been
exhausted.

## Reading Data From KV Bucket

Once a SparkContext is created, we can load data stored in Riak KV buckets into Spark as RDDs. To specify which bucket to use:

**Scala**
```scala
val kv_bucket_name = "test-data"
```

Let's do a simple but very powerful full bucket read. We're going to read the content of entire Riak KV bucket in one command, and it will happen in an efficient partitioned parallel way and get values as Strings:

**Scala**
```scala
val data = sc.riakBucket[String](kv_bucket_name).queryAll()
```

**Python**
```python
data = sc.riakBucket(bucket_name, bucket_type).queryAll()
```

When you know your keys by name, you can pass them in directly:

**Scala**
```scala
val rdd = sc.riakBucket[String](kv_bucket_name).queryBucketKeys("Alice", "Bob", "Charlie")
```

**Python**
```python
rdd = sc.riakBucket(bucket_name, bucket_type).queryBucketKeys("Alice", "Bob", "Charlie")
```

You can also specifiy a range of values (say, from 1 to 5000) defined by a numeric 2i index in Riak if your index is `myIndex`:

**Scala**
```scala
val rdd = sc.riakBucket[String](kv_bucket_name).query2iRange("myIndex", 1L, 5000L)
```

**Python**
```python
rdd = sc.riakBucket(bucket_name, bucket_type).query2iRange("myIndex", 1L, 5000L)
```

You can also specify a set of numeric 2i range values to query by:

**Scala**
```scala
val rdd = sc.riakBucket[String](kv_bucket_name).partitionBy2iRanges("myIndex", 1->3, 4->6, 7->12)
```

**Python**
```python
rdd = sc.riakBucket(bucket_name, bucket_type).partitionBy2iRanges("myIndex", (1, 3), (4, 6), (7, 12))
```

You can also query by a 2i string tag or set of 2i string tags:

**Scala**
```scala
val rdd = sc.riakBucket[String](kv_bucket_name).query2iKeys("dailyDataIndx", "mon_data", "wed_data", "fri_data")
```

**Python**
```python
rdd = sc.riakBucket(bucket_name, bucket_type).query2iKeys("dailyDataIndx", "mon_data", "wed_data", "fri_data")
```

## Writing Data To KV Bucket

To be able to write data out from an RDD into a Riak KV bucket, the following import for a writer is needed:

```scala
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
```

Define the output bucket and issue `saveToRiak` method on an RDD:

**Scala**
```scala
val output_kv_bucket = "test-bucket"
rdd.saveToRiak(output_kv_bucket)
```

**Python**
```python
rdd.saveToRiak(bucket_name, bucket_type)
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

You can use Spark DataFrames on top of an RDD that was created from a KV Bucket. The entry point to programming Spark with the Dataset and DataFrame API is [SparkSession](https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/SparkSession.html) 

```scala
val sparkSession = SparkSession.builder()
     .master(...)
     .appName(...)
     .config(...)
     .getOrCreate()
```

Then import:

```scala    
import sparkSession.implicits._
```

Next, you have to specify a user defined type to allow schema inference using reflection:

```scala
case class UserData(user_id: String, name: String, age: Int, category: String)
```    

Then, you can use the toDF() method on your RDD.

```scala  
val kv_bucket_name = "test-data"
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
df.createOrReplaceTempView("users")
```

and use Spark SQL queries over it.


```scala
sparkSession.sql("select * from users where age >= 50")
```

Another thing you can use are user defined functions (UDFs). First, you have to register a UDF.

```scala
sparkSession.udf.register("stringLength", (s: String) => s.length)
```

After that you can use it in SQL queries   

```scala
sparkSession.sql("select user_id, name, stringLength(name) nameLength from users order by nameLength")
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
val sparkSession = val sparkSession = SparkSession.builder().getOrCreate()
import sparkSession.implicits._
ts_table_name = "test_table"
```
**Python**
```python
sparkSession = SparkSession.builder.getOrCreate()
sc = sparkSession.sparkContext
ts_table_name = "test_table"
```

To read data from existing TS table `test-table` standard SparkSession can be used by providing a special `“org.apache.spark.sql.riak”` data format and using a Riak TS range query:

**Scala**
```scala
val df = sparkSession.read   
 	.option("spark.riak.connection.hosts","riak_host_ip:10017")
  	.format("org.apache.spark.sql.riak")
  	.load(ts_table_name)
	.select(“time”, “col1”, “col2”)
  	.filter(s"time >= CAST($from AS TIMESTAMP) AND time <= CAST($to AS TIMESTAMP) AND  col1= $value1")
```
**Python**
```python
df = sparkSession.read \
	.option("spark.riak.connection.hosts","riak_host_ip:10017") \
  	.format("org.apache.spark.sql.riak") \
  	.load(ts_table_name) \
	.select(“time”, “col1”, “col2”) \
  	.filter(s"time >= CAST($from AS TIMESTAMP) AND time <= CAST($to AS TIMESTAMP) AND  col1= $value1")
```

Schema may or may not be provided using `.schema()` method. If not provided, it will be inferred. Any of the Spark Connector options can be provided in `.option()` or `.options()`.

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

## Spark Datasets With TS Table
Spark Datasets aka strongly typed Dataframes might be created in a very similar manner to the dataframe, there are only two difference: 

* Datasets requires to have an Encoder; builtin encoders for common Scala types and their product types are already available in implicits object, and you only need to import these implicits as follows:      
```scala
import spark.implicits._
```

* the data type should be provided by calling `as()` routine

Here is an example of a Dataset creation:
```scala
import spark.implicits._

case class TimeSeriesData(time: Long, user_id: String, temperature_k: Double)

val ds = sparkSession.read
  .format("org.apache.spark.sql.riak")
  .option("spark.riakts.bindings.timestamp", "useLong")
  .load(bucketName)
  .filter(filterExpression)
  .as[TimeSeriesData]
```

NOTE: There is no Datasets support for Python since Spark does not support this. 

## Partitioning for KV Buckets

### Key Based Partitioning
Querying with the following methods with result in a RDD with single partition:

* query2iKeys(index, keys*)
* queryBucketKeys(keys*)

If 2i is an Int, Long or BigInt, range query will be automatically split into a number of subranges defined by the **spark.riak.input.split.count** option, creating a partition for each subrange.  
* query2iRange(index, from, to)

If 2i is of the other types, a single partition will be created.

For example,
```scala
val conf = new SparkConf()
        .setAppName("My Spark Riak App")
        .set("spark.riak.input.split.count", "10")

val sparkSession = SparkSession.builder()
     .config(sparkConf)
     .getOrCreate()
val sc = sparkSession.sparkContext
...
sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
    .query2iRange(CREATION_INDEX, 100L, 200L)
```
Initial range of [100, 200] will be split into 10 subranges: [100,110], [111,121], [122,132], [133,143], [144,154], [155,165], [166,176], [177,187], [188,198], [199,200].

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

    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .schema(newSchema)
      .load(tableName)
      .filter(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'")
```

You can use `spark.riakts.bindings.timestamp` and Automatic Schema Discovery with `useLong`:

```scala
val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useLong")
      .load(tableName)
      .filter(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
```
In the previous example, the query times, `queryFromMillis` and `queryToMillis`, are Long integers since the datetime values in `df` are stored as Long integers.

Or, you can use `spark.riakts.bindings.timestamp` and Automatic Schema Discovery with `useTimestamp`:

```scala
val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useTimestamp")
      .load(tableName)
      .filter(s"time > CAST('$from' AS TIMESTAMP) AND time < CAST('$to' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")
```
In the previous example, the query times, `CAST('$from' AS TIMESTAMP)` and `CAST('$to' AS TIMESTAMP)`, are Timestamps which are cast from a Long integer since the datetime values in `df` are stored as Timestamps.


## Partitioning for Riak TS Table Queries

For native Riak TS range queries (without Dataframes) single partition will be created. For Dataframe reads there are two options.

### Coverage Plan Based Partitioning

By default all Dataframe reads will use coverage plan based partitioner.
It will split initial range query into subranges each containing period of 1 quantum. All the data from the subrange is stored on the same Riak node. Subranges are then grouped by host and split into a number of partitons(spark.riak.input.split.count) such that each partition queries single Riak node.
This guaranties that queries will not exceed the maximum quanta limit of 5.

NOTE: All data from each subrange will be loaded at once. Paginated reads are not yet implemented.

### Range Query Partitioning

Large ranges can be automatically split into smaller sub-ranges at partitioning time without taking into account data location by simply dividing the initial range into a number of partitions.

To use this functionality it's required to provide the following options:
* `spark.riak.partitioning.ts-range-field-name` to identify quantized field
* `spark.riak.input.split.count` to identify number of partitions/subranges (default value is `10`)

For example:

**Scala**
```scala
   val df = sparkSession.read
      .option("spark.riak.input.split.count", "5")
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(ts_table_name)
      .filter(s"time >= CAST(10000 AS TIMESTAMP) AND time < CAST(210000 AS TIMESTAMP) AND col1 = 'val1'")
```
**Python**
```python
df = sparkSession.read \
      .option("spark.riak.input.split.count", "5") \
      .option("spark.riak.partitioning.ts-range-field-name", "time") \
      .format("org.apache.spark.sql.riak") \
      .schema(schema) \
      .load(ts_table_name) \
      .filter(s"time >= CAST(10000 AS TIMESTAMP) AND time < CAST(210000 AS TIMESTAMP) AND col1 = 'val1'")

```

The initial range query will be split into 5 subqueries (one per each partition) as follows:
* ```time >= CAST(10000 AS TIMESTAMP) AND time < CAST(50000 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(50000 AS TIMESTAMP) AND time < CAST(90000 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(90000 AS TIMESTAMP) AND time < CAST(130000 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(130000 AS TIMESTAMP) AND time < CAST(170000 AS TIMESTAMP) AND col1 = 'val1'```
* ```time >= CAST(170000 AS TIMESTAMP) AND time < CAST(210000 AS TIMESTAMP) AND col1 = 'val1'```

An additional option spark.riak.partitioning.ts-quantum can be passed to notify the Spark-Riak Connector of the quantum size. If the automatically created subranges break the 5 quanta limitation, the initial range will be split into ~4 quantum subranges and the resulting subranges will then be grouped to form the required number of partitions.
**Scala**
```scala
   val df = sparkSession.read
      .option("spark.riak.input.split.count", "5")
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .option("spark.riak.partitioning.ts-quantum", "5s")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(ts_table_name)
      .filter(s"time >= CAST(10000 AS TIMESTAMP) AND time < CAST(210000 AS TIMESTAMP) AND col1 = 'val1'")
```
**Python**
```python
df = sparkSession.read \
      .option("spark.riak.input.split.count", "5") \
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName) \
      .option("spark.riak.partitioning.ts-quantum", "5s") \
      .format("org.apache.spark.sql.riak")
      .schema(schema) \
      .load(ts_table_name) \
      .filter(s"time >= CAST(10000 AS TIMESTAMP) AND time < CAST(210000 AS TIMESTAMP) AND col1 = 'val1'")

```
Splitting [10000, 210000) into 5 partitions will give a period of 40000ms per partition, which will hit the quanta limitation when querying Riak TS.
In this case, the initial range will be spit into 5 sets of 2 subranges:
* ```[(time >= 10000 AND time < 30000), (time >= 30000 AND time < 50000)]```
* ```[(time >= 50000 AND time < 70000), (time >= 70000 AND time < 90000)]```
* ```[(time >= 90000 AND time < 110000), (time >= 110000 AND time < 130000)]```
* ```[(time >= 130000 AND time < 150000), (time >= 150000 AND time < 170000)]```
* ```[(time >= 170000 AND time < 190000), (time >= 190000 AND time < 210000)]```

Not providing the `spark.riak.partitioning.ts-range-field-name` property will default to having a single partition with initial query.

NOTE: All data from each subrange will be loaded at once. Paginated reads are not yet implemented.

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
val df = sparkSession.write
	.option("spark.riakts.write.bulk-size", "500")
      	.format("org.apache.spark.sql.riak")
      	.mode(SaveMode.Append)
      	.save(bucketName)
```
**Python**
```python
df = sparkSession.write
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

## Using Jupyter Notebook

To use the Spark-Riak connector with Jupyter notebook you install Jupyter Notebook(you can follow [these](http://jupyter.readthedocs.io/en/latest/install.html) instructions or use the official Docker image [image](https://hub.docker.com/r/jupyter/pyspark-notebook/)). Once installed, start Jupyter with `jupyter notebook`. Then execute the following code to initialize a Spark context:

```python
import findspark
findspark.init()
import pyspark
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages com.basho.riak:spark-riak-connector_2.10:1.6.0 pyspark-shell"
conf = pyspark.SparkConf().setAppName("My Spark Riak App")
conf.set("spark.riak.connection.host", "127.0.0.1:8087")
sc = pyspark.SparkContext(conf=conf)
```

Now you have a Spark context that is ready to use with Riak and running inside a Jupyter notebook.

## Spark Streaming

As stated in the [official Spark Streaming documentation](http://spark.apache.org/docs/latest/streaming-programming-guide.html), "Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Flume, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like map, reduce, join and window."

### The Basic Idea

#### Spark Streaming

Here is a basic Spark Streaming sample which writes to the console with `errorlines.print()`:

Create a StreamingContext with a SparkConf configuration:

```scala
    val ssc = new StreamingContext(sparkConf, Seconds(1))
```

Create a DStream that will connect to serverIP:serverPort:

```scala
    val lines = ssc.socketTextStream(serverIP, serverPort)
```

Count all lines with 'ERROR' in each batch:

```scala
    val errorlines = lines.filter(lines => lines contains "ERROR")
    val errorcount = errorlines.count()
    println(errorcount)
```

Print a few of the error lines to the console and start the stream:
```scala
    errorlines.print()
    ssc.start()  
    ssc.awaitTermination() // Wait for the computation to terminate
```

#### Spark Streaming With Riak

To add Riak related features to the `StreamingContext` and `RDD` it is required to add some packages into the scope:

```scala
    import com.basho.riak.spark.streaming._
```

And after that we can simply replace the print statement with a `saveToRiak` call:
```scala
    errorlines.saveToRiak("test-bucket-4store")
```

### Setting up Streaming

#### Create A 'StreamingContext'

The streaming context has only two parameters which are required for minimal configuration. The first one is `sparkConf`. Please see [how to create and use `SparkConf`](using-connector.md#configuration-of-spark-context) for more information. The second required parameter is the `batchDuration` which sets the interval in which streaming data will be divided into batches: Note the Spark API provides a Milliseconds, Seconds, Minutes, all of which are accepted as a `Duration`. This `Duration` is not to be confused with the [scala.concurrent.duration.Duration](http://www.scala-lang.org/api/current/index.html#scala.concurrent.duration.Duration)

```scala
    val ssc = new StreamingContext(conf, Seconds(n))
```

#### Creating A Stream

You can create any of the available Spark streams or customize your own streams. Please see [this section](http://spark.apache.org/docs/latest/streaming-programming-guide.html#basic-sources) from the Spark documentation for more details.

#### Enable Spark Streaming With Riak

The following enables Riak-related functions on the `StreamingContext`, `DStream` and `RDD`:

```scala
    import com.basho.riak.spark.streaming._
```

#### Writing to Riak From A Stream

The data can be stored either to KV or TS storage.

Saving data to KV:
```scala
    stream.saveToRiak(DEFAULT_NAMESPACE_4STORE.getBucketNameAsString)
    ssc.start() // start computation
```

The difference between writing to KV and TS is really small. To meet the requirements of TS, it is necessary to convert data into Spark Row object.
Saving data to TS:
```scala
    stream
        .map(data => Row(...)
        .saveToRiak(DEFAULT_NAMESPACE_4STORE.getBucketNameAsString)
    ssc.start() // start computation
```

#### Reading From Riak From The `StreamingContext`

Since Riak-specific `StreamingContext` extends usual Riak-specific `SparkContext`, it is possible to read data from Riak using `StreamingContext`:
```scala
    var rdd = ssc.riakBucket(DEFAULT_NAMESPACE_4STORE).queryAll()
```

### Find out more
http://spark.apache.org/docs/latest/streaming-programming-guide.html

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
