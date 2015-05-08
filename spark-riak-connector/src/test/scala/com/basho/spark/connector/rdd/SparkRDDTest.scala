package com.basho.spark.connector.rdd

import com.basho.riak.client.core.query.{Namespace, RiakObject, Location}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.spark.connector.util.RiakObjectConversionUtil
import com.basho.spark.connector.writer.{ValueWriter, ValueWriterFactory}

import org.apache.spark.rdd.RDD
import org.junit.{Ignore, Before, Test}
import com.basho.spark.connector._
import org.junit.Assert._

import scala.collection.mutable.ListBuffer

case class UserData(timestamp: String, user_id: String)

class SparkRDDTest extends AbstractRDDTest {
  private val CREATION_INDEX = "creationNo"

  protected override def jsonData(): String =
    "[" +
      "  {key: 'key-1', indexes: {creationNo: 1}, value: {timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}}" +
      ", {key: 'key-2', indexes: {creationNo: 2}, value: {timestamp: '2014-11-24T13:15:04.823Z', user_id: 'u1'}}" +
      ", {key: 'key-3', indexes: {creationNo: 3}, value: {timestamp: '2014-11-24T13:18:04', user_id: 'u1'}}" +
      ", {key: 'key-4', indexes: {creationNo: 4}, value: {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}}" +
      ", {key: 'key-5', indexes: {creationNo: 5}, value: {timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}}" +
      ", {key: 'key-6', indexes: {creationNo: 6}, value: {timestamp: '2014-11-24T13:21:04.823Z', user_id: 'u3'}}" +
    "]"

  var rdd: RDD[UserData] = null

  protected override def initSparkConf() =
    super.initSparkConf()
      .setAppName("RDD tests")

  @Before
  def initializeRDD(): Unit ={
    rdd = sc.riakBucket[UserData](DEFAULT_NAMESPACE.getBucketNameAsString)
      .query2iRange(CREATION_INDEX, 1, 6)
  }

  @Test
  def calculateCount(){
    val count = rdd.count()
    assertEquals(6, count)
  }

  @Test
  def firstElement(): Unit ={
    // Initial implementation fails on this operation
    val first = rdd.first()
  }

  /**
   * Returns PairRDD which consist of the two fields:
   *  user_id
   *  calculated total number of entries for that user.
   *
   * The RDD is sorted by teh second field:
   *   (u2, 1)
   *   (u3, 2)
   *   (u1, 3)
   */
  private def calculateUserOrderedTotals() = {
    rdd.map(x => (x.user_id, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._1)
  }

  private def fetchAllFromBucket(ns:Namespace): List[(String,String)] = {
    val data =  ListBuffer[(String,String)]()
    withRiakDo(session=>
      foreachKeyInBucket(session, ns, (client, l: Location) =>{
        val v = readByLocation[String](client, l)
        data += ((l.getKeyAsString,v))
        false
      })
    )
    data.toList
  }

  @Test
  def checkActions(): Unit ={
    val perUserTotalRDD = calculateUserOrderedTotals()
    val data = perUserTotalRDD.collect()
    assertEqualsUsingJSON("[['u1',3],['u2',1],['u3',2]]", data)
  }

  @Test
  def readDataForSpecifiedBucketKeys(): Unit = {
    rdd = sc.riakBucket(DEFAULT_NAMESPACE)


  }

    @Test
  def storePairRDDUsingCustomMapper(): Unit = {
    /**
     * RDD contains the following data:
     *   (u2, 1)
     *   (u3, 2)
     *   (u1, 3)
     */
    val perUserTotals = calculateUserOrderedTotals()

    /**
     * Custom value writer factory which uses totals as a key.
     */
    implicit val vwf = new ValueWriterFactory[(String,Int)]{
      override def valueWriter(bucket: BucketDef): ValueWriter[(String, Int)] = {
        new ValueWriter[(String, Int)] {
          override def mapValue(value: (String, Int)): (BinaryValue, RiakObject) = {
            (BinaryValue.create(value._2.toString), RiakObjectConversionUtil.to(value._1))
          }
        }
      }
    }

    perUserTotals.saveAsRiakBucket(BucketDef(DEFAULT_NAMESPACE_4STORE))

    val data = fetchAllFromBucket(DEFAULT_NAMESPACE_4STORE)

    // Since Riak may returns results in any order, we need to ignore order at all
    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "['2','u3']," +
        "['3','u1']," +
        "['1','u2']" +
      "]", data)
  }

  @Ignore("Need to fix Tuple2 desiarilization")
  @Test
  def storePairRDDWithDefaultMapper(): Unit = {
    val perUserTotalRDD = calculateUserOrderedTotals()
    perUserTotalRDD.saveToRiak(DEFAULT_NAMESPACE_4STORE.getBucketNameAsString)

    // Read data from riak and populate data buffer
    val data =  ListBuffer[(String,Long)]()
    RiakConnector(sc.getConf).withSessionDo { session => {
      foreachKeyInBucket(session, DEFAULT_NAMESPACE_4STORE, (RiakConnector, l: Location) =>{
        val v = readByLocation[Long](session, l)
        data += ((l.getKeyAsString,v))
        false
      })
    }}
    assertEquals(3, data.size)
  }
}
