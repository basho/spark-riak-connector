package com.basho.riak.spark.rdd.timeseries

import com.basho.riak.client.api.commands.timeseries.Delete
import com.basho.riak.client.core.query.timeseries.Cell
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.RiakTSTests
import com.basho.riak.spark.writer.WriteDataMapperFactory._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.junit.experimental.categories.Category

import scala.collection.JavaConversions._

@Category(Array(classOf[RiakTSTests]))
class TimeSeriesWriteTest extends AbstractTimeSeriesTest {

  @Before
  def setUp(): Unit = {
    val from = tsRangeStart.getTimeInMillis - 5
    val to = tsRangeEnd.getTimeInMillis + 10
    val bucketName = DEFAULT_TS_NAMESPACE.getBucketTypeAsString

    sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT surrogate_key, family, time, user_id, temperature_k FROM $bucketName WHERE time > $from AND time < $to AND surrogate_key = 1 AND family = 'f'")
      .collect()
      .map(row => (row.getLong(0), row.getString(1), row.getLong(2)))
      .map { case (surrogateKey, family, time) => List(new Cell(surrogateKey), new Cell(family), Cell.newTimestamp(time)) }
      .foreach { keys =>
        val builder = new Delete.Builder(DEFAULT_TS_NAMESPACE.getBucketNameAsString(), keys)
        withRiakDo(_.execute(builder.build()))
      }
  }

  @Test
  def saveSqlRowsToRiak(): Unit = {
    val from = tsRangeStart.getTimeInMillis - 5
    val to = tsRangeEnd.getTimeInMillis + 10
    val bucketName = DEFAULT_TS_NAMESPACE.getBucketTypeAsString

    val sqlRowsRdd = sc.parallelize(
      Seq(
        Row(1, "f", 111111L, "bryce", 305.37),
        Row(1, "f", 111222L, "bryce", 300.12),
        Row(1, "f", 111333L, "bryce", 295.95),
        Row(1, "f", 111444L, "ratman", 362.121),
        Row(1, "f", 111555L, "ratman", 3502.212)))
    sqlRowsRdd.saveToRiakTS(DEFAULT_TS_NAMESPACE.getBucketTypeAsString)

    val newRdd = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT user_id, temperature_k FROM $bucketName WHERE time > $from AND time < $to AND surrogate_key = 1 AND family = 'f'")
    assertEquals(5, newRdd.count())

    val data = newRdd.collect().map(_.toSeq)

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   ['bryce',305.37],
        |   ['bryce',300.12],
        |   ['bryce',295.95],
        |   ['ratman',362.121],
        |   ['ratman',3502.212]
        |]
      """.stripMargin, data)
  }

  @Test
  def saveDataFrameWithSchemaToRiak(): Unit = {
    val from = tsRangeStart.getTimeInMillis - 5
    val to = tsRangeEnd.getTimeInMillis + 10
    val bucketName = DEFAULT_TS_NAMESPACE.getBucketTypeAsString

    val sqlContext = new SQLContext(sc)

    val jsonRdd = sc.parallelize(Seq(
      """{"surrogate_key": 1, "family": "f", "time": 111111, "user_id": "bryce", "temperature_k": 305.37}""",
      """{"surrogate_key": 1, "family": "f", "time": 111222, "user_id": "bryce", "temperature_k": 300.12}""",
      """{"surrogate_key": 1, "family": "f", "time": 111333, "user_id": "bryce", "temperature_k": 295.95}""",
      """{"surrogate_key": 1, "family": "f", "time": 111444, "user_id": "ratman", "temperature_k": 362.121}""",
      """{"surrogate_key": 1, "family": "f", "time": 111555, "user_id": "ratman", "temperature_k": 3502.212}"""))

    val df = sqlContext.read.schema(
      StructType(List(
        StructField(name = "surrogate_key", dataType = IntegerType),
        StructField(name = "family", dataType = StringType),
        StructField(name = "time", dataType = LongType),
        StructField(name = "user_id", dataType = StringType),
        StructField(name = "temperature_k", dataType = DoubleType))))
      .json(jsonRdd)

    df.rdd.saveToRiakTS(DEFAULT_TS_NAMESPACE.getBucketTypeAsString)

    val newRdd = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT user_id, temperature_k FROM $bucketName WHERE time > $from AND time < $to AND surrogate_key = 1 AND family = 'f'")
    assertEquals(5, newRdd.count())

    val data = newRdd.collect().map(_.toSeq)

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   ['bryce',305.37],
        |   ['bryce',300.12],
        |   ['bryce',295.95],
        |   ['ratman',362.121],
        |   ['ratman',3502.212]
        |]
      """.stripMargin, data)
  }

}
