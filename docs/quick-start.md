# Quick Start Guide

This guide will run you through a quick example that uses the Spark-Riak Connector to read and write data using Java, Scala, and Python. We will assume you are running this guide on Mac OSX. 

## Prerequisites

- Update brew with `brew update`.
- Install Riak TS OSX build. Instruction can be found [here](http://docs.basho.com/riak/ts/1.2.0/installing/mac-osx/)
- Set open file limits for Riak by following the guide [here](http://docs.basho.com/riak/latest/ops/tuning/open-files-limit/#Mac-OS-X).
- Install Spark with `brew install apache-spark`.
- Download the Spark-Riak Connector jar file [here](https://bintray.com/artifact/download/basho/data-platform/com/basho/riak/spark-riak-connector/1.3.0-beta1/spark-riak-connector-1.3.0-beta1-uber.jar).

Start Riak TS with `riak start`.

Scroll down or click below to find the desired quick start guide:
- [Scala](./quick-start.md#scala)
- [Python](./quick-start.md#python)
- [Java](./quick-start.md#java)

## Scala

In this quick start guide we will run you through some examples usages of the Spark-Riak Connector using the Spark Scala REPL.

Start Spark Scala REPL with: 

```
path/to/spark-shell \
--conf spark.riak.connection.host=127.0.0.1:8087 \
--driver-class-path /path/to/spark-riak-connector-1.3.0-beta1-uber.jar
```

Import the following:

```scala
import com.basho.riak.client.core.query.Namespace
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.riak.spark._
```

Create an RDD with some test data and save to Riak KV bucket:

```scala
val data = Array(1, 2, 3, 4, 5)
val testRDD = sc.parallelize(data)
testRDD.saveToRiak(new Namespace("kv_bucket_a"))
```

When saving RDD's to Riak that only have values (i.e not key-value pairs), keys will be automatically generated for each value in the RDD.

Query Riak TS bucket and print results:

```scala
val queryRDD = sc.riakBucket[String](new Namespace("kv_bucket_a")).queryAll()
queryRDD.collect().foreach(println)
```

You should see:

```scala
scala> queryRDD.collect().foreach(println)
5
4
3
1
2
```

You will notice that all values (1,2,3,4,5) are printed.

Create a key-value pair RDD and save it to Riak KV bucket:

```scala
val data = Array((1,2), (2,4), (3,6), (4,8), (5,10))
val testRDD = sc.parallelize(data)
testRDD.saveToRiak(new Namespace("kv_bucket_b"))
```

Query Riak KV bucket and print results:

```scala
val queryRDD = sc.riakBucket[String](new Namespace("kv_bucket_b")).queryAll()
queryRDD.collect().foreach(println)
```

You should see:

```scala
scala> queryRDD.collect().foreach(println)
4
6
10
2
8
```

You will notice only 2, 4, 6, 8, and 10 are printed. When using tuples, the first value is the key and the second value is the value.

Query Riak KV bucket by keys:

```scala
val keyRDD = sc.riakBucket[String](new Namespace("kv_bucket_b")).queryBucketKeys("1", "2", "3")
keyRDD.collect().foreach(println)
```

You should see:

```scala
scala> keyRDD.collect().foreach(println)
4
6
2
```

You will notice that this prints out (2,4,6).

If your data contains 2i secondary indices you can query by that too:

```scala
val rangeRDD = sc.riakBucket[String](new Namespace("kv_bucket_b")).query2iRange("myIndex", 1L, 5000L)
rangeRDD.collect().foreach(println)
```

Since none of our test data had a secondary index associated with it, our rangeRDD will be empty.

Now let's create a Spark SQL DataFrame and write it to a Riak TS table.

Make the following imports:

```scala
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp
import com.basho.riak.spark.rdd.connector.RiakConnector
```

Then set up Spark sqlContext:

```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
```

Create an RDD with some data:

```scala
val testRDD = sc.parallelize(Seq(
  (1, "f", Timestamp.valueOf("1980-1-1 10:00:00"), "v1"),
  (1, "f", Timestamp.valueOf("1980-1-1 10:10:00"), "v2"),
  (1, "f", Timestamp.valueOf("1980-1-1 10:20:00"), "v3")))
```

Convert the RDD to a Spark SQL DataFrame and look at the schema:

```scala
val df = testRDD.toDF("k", "family", "ts", "value")
df.printSchema()
```

Then, create a TS table with the same data format as the testRDD that we created:

```scala
val tableName = "ts_table_c"
val connector = RiakConnector(sc.getConf)

connector.withSessionDo(session =>{
          val request = new com.basho.riak.client.api.commands.timeseries.Query.Builder(
            s"""
              |   CREATE TABLE $tableName  (
              |       k       SINT64    not null,
              |       family  VARCHAR   not null,
              |       ts      TIMESTAMP not null,
              |       value   VARCHAR,
              |
              |       primary key ((k, family, quantum(ts,1,h)), k, family, ts)
              |   )
              |
            """.stripMargin)
            .build()

val response = session.execute(request)})
```

Now, write the Spark SQL DataFrame to the newly created Riak TS table:

```scala
df.write.format("org.apache.spark.sql.riak").mode(SaveMode.Append).save(tableName)
```

And, finally, check that the table was successfully written into the Riak TS table by making a simple query and printing the result:

```scala

val test_query = "ts >= CAST('1980-1-1 10:00:00' AS TIMESTAMP) AND ts <= CAST('1980-1-1 10:30:00' AS TIMESTAMP) AND k = 1 AND family = 'f'" 

val df2 = sqlContext.read.format("org.apache.spark.sql.riak").load(tableName).filter(test_query)

df2.show()
```


### Python

In this quick start guide we will run  through some examples usages of the Spark-Riak Connector using the Spark Python REPL, pyspark.

Start pyspark with: 

```
/path/to/bin/pyspark \
--conf spark.riak.connection.host=127.0.0.1:8087 \
--driver-class-path /path/to/spark-riak-connector-1.3.0-beta1-uber.jar 
```

Make some imports:

```python
import riak, datetime, time, random
```

Set up Riak TS connection:

```python
host='127.0.0.1'
pb_port = '8087'
hostAndPort = ":".join([host, pb_port])
client = riak.RiakClient(host=host, pb_port=pb_port)
table_name = 'pyspark-%d' % int(time.time())
table = client.table(table_name)
```

Create a TS table:

```python
create_sql = """CREATE TABLE %(table_name)s (
site varchar not null,
species varchar not null,
measurementDate timestamp not null,
value double, 
PRIMARY KEY ((site, species, quantum(measurementDate, 24, h)),
    site, species, measurementDate))
""" % ({'table_name': table_name})
result = table.query(create_sql)
```

Print the schema:

```python
schema = table.describe().rows
for r in schema:
    print r
```

You should see:
```
['site', 'varchar', False, 1L, 1L]
['species', 'varchar', False, 2L, 2L]
['measurementDate', 'timestamp', False, 3L, 3L]
['value', 'double', True, None, None]
```

Generate and print some data:

```python
site = 'AA'
species = 'fff'
start_date = int(time.time())
events = []
for i in range(9):
    measurementDate = start_date + i
    value = random.uniform(-20, 110)
    events.append([site, species, measurementDate, value])

end_date = measurementDate 

for e in events:
    print e
```

You should see something like this:

```
['AA', 'fff', 1460147465, 84.2863373359625]
['AA', 'fff', 1460147466, 22.460677478919976]
['AA', 'fff', 1460147467, 99.44873894866066]
['AA', 'fff', 1460147468, 79.22655985587694]
['AA', 'fff', 1460147469, 20.37795468066598]
['AA', 'fff', 1460147470, 77.30363887094994]
['AA', 'fff', 1460147471, 77.48514266033274]
['AA', 'fff', 1460147472, 78.94730225284083]
['AA', 'fff', 1460147473, 29.09084815136098]
```

Create an RDD from the generated data:

```python
testRDD = sc.parallelize(events)
```

Take this RDD and convert it to a DataFrame and rename the columns to match the Riak TS table:

```python
df = testRDD.toDF(['site', 'species','measurementDate','value'])
df.show()
```

You should see something like this:

```
+----+-------+---------------+------------------+
|site|species|measurementDate|             value|
+----+-------+---------------+------------------+
|  AA|    fff|     1460147465|  84.2863373359625|
|  AA|    fff|     1460147466|22.460677478919976|
|  AA|    fff|     1460147467| 99.44873894866066|
|  AA|    fff|     1460147468| 79.22655985587694|
|  AA|    fff|     1460147469| 20.37795468066598|
|  AA|    fff|     1460147470| 77.30363887094994|
|  AA|    fff|     1460147471| 77.48514266033274|
|  AA|    fff|     1460147472| 78.94730225284083|
|  AA|    fff|     1460147473| 29.09084815136098|
+----+-------+---------------+------------------+
```

Write the DataFrame to the TS table:

```python

df.write \
    .format('org.apache.spark.sql.riak') \
    .option('spark.riak.connection.host', hostAndPort) \
    .mode('Append') \
    .save(table_name) 
```

Lets check that the write was successful by reading the TS table into a new DataFrame:

```python
sqlContext = SQLContext(sc)
df2 = sqlContext.read\
    .format("org.apache.spark.sql.riak")\
    .option("spark.riak.connection.host", hostAndPort)\
    .option("spark.riakts.bindings.timestamp", "useLong")\
    .load(table_name)\
    .filter("""measurementDate > %(start_date)s
        AND measurementDate <  %(end_date)s
        AND site = '%(site)s'
        AND species = '%(species)s'
    """ % ({'start_date': start_date, 'end_date': end_date, 'site': site, 'species': species}))
```

Print the table schema:

```python
df2.printSchema()
```

You should see:

```
root
 |-- site: string (nullable = false)
 |-- species: string (nullable = false)
 |-- measurementDate: long (nullable = false)
 |-- value: double (nullable = true)
```

Show the DataFrame:

```
df2.show()
```

You should see something like this:

```
+----+-------+---------------+------------------+
|site|species|measurementDate|             value|
+----+-------+---------------+------------------+
|  AA|    fff|     1460147466|22.460677478919976|
|  AA|    fff|     1460147467| 99.44873894866066|
|  AA|    fff|     1460147468| 79.22655985587694|
|  AA|    fff|     1460147469| 20.37795468066598|
|  AA|    fff|     1460147470| 77.30363887094994|
|  AA|    fff|     1460147471| 77.48514266033274|
|  AA|    fff|     1460147472| 78.94730225284083|
+----+-------+---------------+------------------+
```

Register the DataFrame as a temp sql table and run a sql query to obtain the average of the "value" column:
 
 ```python
df2.registerTempTable("pyspark_tmp")
sqlContext.sql("select avg(value) as average_value from pyspark_tmp").show()
 ```
 
You should see something similar to this:

```
+-----------------+
|    average_value|
+-----------------+
|65.03571639260672|
+-----------------+
```

### Java

Coming Soon!
