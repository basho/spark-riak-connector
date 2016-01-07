package com.basho.riak.spark.rdd.timeseries

import java.util.concurrent.ExecutionException
import java.util.{Calendar, GregorianCalendar, TimeZone}

import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.operations.FetchBucketPropsOperation
import com.basho.riak.client.core.operations.ts.StoreOperation
import com.basho.riak.client.core.query.timeseries.{Cell, Row}
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.rdd.AbstractRDDTest
import org.junit.{Assume, Before}

import scala.collection.JavaConversions._

case class TimeSeriesData(time: Long, user_id: String, temperature_k: Double)

abstract class AbstractTimeSeriesTest extends AbstractRDDTest {

  protected val DEFAULT_TS_NAMESPACE = new Namespace("time_series_test","time_series_test")

  var tsRangeStart: Calendar = null
  var tsRangeEnd: Calendar = null

  final val msg = "Bucket type for Time Series test data is not created, Time series tests will be skipped"

  protected def mkTimestamp(timeInMillis: Long): Calendar = {
    val c = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    c.setTimeInMillis(timeInMillis)
    c
  }

  override def initialize(): Unit = {
    /* DO NOT CALL IHERITED initialize to avoid reseting non TS bucket */
    // super.initialize()
    sc = createSparkContext(initSparkConf())
  }

  @Before
  def setupData(): Unit = {
    checkBucketExistence(DEFAULT_TS_NAMESPACE, msg + "\n\n" +
      "To create and activate TS test bucket, please use the following commands:\n" +
      "\t" + """./riak-admin bucket-type create time_series_test '{"props":{"n_val":3, "table_def": "create table time_series_test (surrogate_key sint64 not null, family varchar not null, time timestamp not null, user_id varchar not null, temperature_k double, primary key ((surrogate_key, family, quantum(time, 10, s)), surrogate_key, family, time))"}}' """  +"\n" +
      "\t./riak-admin bucket-type activate time_series_test")

    // ----------  Storing test data into Riak TS

    val testData = List(
      TimeSeriesData(111111, "bryce", 305.37),
      TimeSeriesData(111222, "bryce", 300.12),
      TimeSeriesData(111333, "bryce", 295.95),

      TimeSeriesData(111444, "ratman", 362.121),
      TimeSeriesData(111555, "ratman", 3502.212)
    )

    tsRangeStart = mkTimestamp(testData.minBy(_.time).time)
    tsRangeEnd = mkTimestamp(testData.maxBy(_.time).time)

    val tableName = DEFAULT_TS_NAMESPACE.getBucketType

    val rows = testData.map(f => new Row(
      new Cell(1) /*surrogate_key*/,
      new Cell("f") /* family */,
      Cell.newTimestamp(f.time),
      new Cell(f.user_id),
      new Cell(f.temperature_k)))

    val storeOp = new StoreOperation.Builder(tableName).withRows(rows).build()

    withRiakDo(session=>{
      session.getRiakCluster.execute(storeOp).get()
    })
  }

  protected def checkBucketExistence(ns: Namespace, warningMsg: String): Unit = {
    val fetchProps = new FetchBucketPropsOperation.Builder(ns).build()

    withRiakDo( session => {
      session.getRiakCluster.execute(fetchProps)
    })

    try {
      fetchProps.get().getBucketProperties
    } catch {
      case ex :ExecutionException if ex.getCause.isInstanceOf[RiakResponseException]
        && ex.getCause.getMessage.startsWith("No bucket-type named")  =>
        logWarning(warningMsg)
        Assume.assumeTrue(msg + " (See logs for the details)", false)
    }
  }

}
