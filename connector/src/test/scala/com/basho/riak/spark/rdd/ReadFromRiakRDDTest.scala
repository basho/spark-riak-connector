/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.rdd

import java.math.BigInteger
import java.util.UUID
import org.junit.Assert._
import org.junit.Test
import org.junit.experimental.categories.Category
import com.basho.riak.spark._

case class UserTS(timestamp: String, user_id: String)

@Category(Array(classOf[IntegrationTests]))
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
  @Category(Array(classOf[RiakCommonTests]))
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
  @Category(Array(classOf[RiakCommonTests]))
  def readNothing_if_noDataFound(): Unit ={
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .queryBucketKeys(UUID.randomUUID().toString)

    val data = rdd.collect()

    assertEquals(0, data.length)
  }

  @Test
  @Category(Array(classOf[RiakCommonTests]))
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
  @Category(Array(classOf[RiakCommonTests]))
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
  @Category(Array(classOf[RiakCommonTests]))
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
  @Category(Array(classOf[RiakCommonTests]))
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
  @Category(Array(classOf[RiakCommonTests]))
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

  @Test
  @Category(Array(classOf[RegressionTests]))
  def readJSONValueWrittenWithCharsetInContentType(): Unit ={

    val (key, ro) = createRiakObjectFrom("{key: 'value-with-charset', value: {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}}")
    ro.setContentType("application/json;charset=UTF-8")

    withRiakDo(session=>{
      createValueRaw(session, DEFAULT_NAMESPACE, ro, key)
    })


    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
        .queryBucketKeys(key)
        .collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "{user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
      "]",
      data)
  }

  @Test
  @Category(Array(classOf[RiakCommonTests]))
  def readUDT_2iJavaBigIntegerKeysRange(): Unit = {
    val rdd = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, new BigInteger("1"), new BigInteger("3"))

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
  @Category(Array(classOf[RiakTSTests], classOf[RiakBDPTests]))
  def local2iRangeRead() ={

    val data = sc.riakBucket[UserTS](DEFAULT_NAMESPACE)
      .query2iRangeLocal("creationNo", 1, 1000)
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx, preservesPartitioning=true)
      .groupByKey()
      .collect()

    val count = data.size

    // TODO: needs to verify the number of the created partitions
    val allValues = data.map{case (k,v)=> v}.flatten

    assertEquals(7, allValues.size)
  }
}
