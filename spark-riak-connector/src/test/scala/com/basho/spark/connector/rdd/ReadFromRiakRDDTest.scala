package com.basho.spark.connector.rdd

import java.util.UUID

import org.junit.Test
import org.junit.Assert._

import com.basho.spark.connector._
import org.junit.experimental.categories.Category

case class UserTS(timestamp: String, user_id: String)

class ReadFromRiakRDDTest extends AbstractRDDTest{
  private val CREATION_INDEX = "creationNo"

  protected override def jsonData(): String =
    "[" +
      " { key: 'key-1', indexes: {creationNo: 1, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}}" +
      ",{ key: 'key-2', indexes: {creationNo: 2, category: 'visitor'}, value:  {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}}" +
      ",{ key: 'key-3', indexes: {creationNo: 3, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}}" +
      ",{ key: 'key-4', indexes: {creationNo: 4, category: 'stranger'}, value: {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}}" +
      ",{ key: 'key-5', indexes: {creationNo: 5, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}}" +
      ",{ key: 'key-6', indexes: {creationNo: 6, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}}" +
      ",{ key: 'key-7', indexes: {creationNo: 7, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}}" +
    "]"

  protected override def initSparkConf() =
    super.initSparkConf()
      .setAppName("Bunch of read RDD tests")

  @Test
  @Category(Array(classOf[RegressionTests]))
  def readDataBySeveralChunk_when_lastChunkIsFilledCompletely(): Unit ={
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, 1, 4)

    val data = rdd.collect()
    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "{timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}," +
        "{timestamp: '2014-11-24T13:15:04.824Z', user_id: 'u1'}," +
        "{timestamp: '2014-11-24T13:18:04', user_id: 'u1'}," +
        "{timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}" +
      "]"
      , data)
  }

  @Test
  def readUDT_2iKeysRange(): Unit = {
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, 1, 3)

    val data = rdd.collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "{timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}," +
        "{timestamp: '2014-11-24T13:15:04.824Z', user_id: 'u1'}," +
        "{timestamp: '2014-11-24T13:18:04', user_id: 'u1'}" +
      "]"
      , data)
  }

  @Test
  @Category(Array(classOf[RegressionTests]))
  def readNothing_if_query2iKeysRangeForNotExistedIndex(): Unit = {
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iRange(UUID.randomUUID().toString, 1, 3)

    val data = rdd.collect()
    assertEquals(0, data.length)
  }

  @Test
  def readNothing_if_noDataFound(): Unit ={
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .queryBucketKeys(UUID.randomUUID().toString)

    val data = rdd.collect()

    assertEquals(0, data.length)
  }

  @Test
  def readUDT_specifiedBucketKeys(): Unit ={
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .queryBucketKeys("key-1", "key-4", "key-6")

    val data = rdd.collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "{timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}," +
        "{timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}," +
        "{timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}" +
      "]"
      , data)
  }

  @Test
  def readUDT_specified2iStringKeys(): Unit = {
    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iKeys("category", "stranger", "visitor")
      .collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "{timestamp: '2014-11-24T13:15:04.824Z', user_id: 'u1'}" +
        ", {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}" +
        ", {timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}" +
        ", {timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}" +
        ", {timestamp: '2014-11-24T12:01:04.825Z', user_id: 'u3'}" +
      "]",
      data
    )
  }

  @Test
  def readUDT_specified2iIntKeys(): Unit = {
    val keys = List(1,3,5)

    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iKeys(CREATION_INDEX, keys: _*)
      .collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "{timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}" +
        ",{timestamp: '2014-11-24T13:18:04', user_id: 'u1'}" +
        ",{timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}" +
        "]",
      data
    )
  }

  /*
   * map RDD[K] to RDD[(partitionIdx,K)]
   */
  val funcReMapWithPartitionIdx = new Function2[Int,Iterator[UserTS], Iterator[(Int,UserTS)]] with Serializable{
    override def apply(partitionIdx: Int, iter: Iterator[UserTS]): Iterator[(Int, UserTS)] = {
      iter.toList.map(x => partitionIdx -> x).iterator
    }
  }

  @Test
  def partitionByInteger2iKeyRanges(): Unit = {

    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .partitionBy2iRanges(CREATION_INDEX, 1->3, 4->6, 7->12)
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx, preservesPartitioning=true)
      .groupByKey()
      .collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +

        // The 1st partition should contains first 3 item
        "[0, [" +
        "     {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}" +
        "     ,{user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
        "     ,{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
        "]]" +

        // The 2nd partition should contsins items; 4,5,6
        ",[1, [" +
        "     {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
        "     ,{user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}" +
        "     ,{user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}" +
        "]]" +

        // The 3rd partition should contains the only 7th item
        ",[2, [" +
        "     {user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}" +
        "]]" +
      "]",
      data
    )
  }

  @Test
  def partitionByString2iKeys(): Unit = {
    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .partitionBy2iKeys("category", "neighbor", "visitor", "stranger")
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx, preservesPartitioning=true)
      .groupByKey()
      .collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +

        // 1st partition should contains 2 neighbors
        "[0, [" +
        " {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}" +
        " ,{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
        "]]" +

        // 2nd partition should contains 1 visitor
        ",[1, [" +
        " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
        "]]" +

        // 3rd partition should contains 4 strangers
        ",[2, [" +
        " {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
        " ,{user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}" +
        " ,{user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}" +
        " ,{user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}" +
        "]]" +
        "]",
      data
    )
  }
}
