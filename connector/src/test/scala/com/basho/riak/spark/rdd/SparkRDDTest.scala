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

import com.basho.riak.client.core.query.Location
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.connector.RiakConnector
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.{Before, Test}

import scala.collection.mutable.ListBuffer

case class UserData(timestamp: String, user_id: String)

@Category(Array(classOf[RiakCommonTests]))
class SparkRDDTest extends AbstractRiakSparkTest {
  private val CREATION_INDEX = "creationNo"

  protected override val jsonData = Option(
    """ [
      |   {key: 'key-1', indexes: {creationNo: 1}, value: {timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}},
      |   {key: 'key-2', indexes: {creationNo: 2}, value: {timestamp: '2014-11-24T13:15:04.823Z', user_id: 'u1'}},
      |   {key: 'key-3', indexes: {creationNo: 3}, value: {timestamp: '2014-11-24T13:18:04', user_id: 'u1'}},
      |   {key: 'key-4', indexes: {creationNo: 4}, value: {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}},
      |   {key: 'key-5', indexes: {creationNo: 5}, value: {timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}},
      |   {key: 'key-6', indexes: {creationNo: 6}, value: {timestamp: '2014-11-24T13:21:04.823Z', user_id: 'u3'}}
      | ]
    """.stripMargin)

  var rdd: RDD[UserData] = _

  protected override def initSparkConf() = super.initSparkConf()
      .setAppName("RDD tests")

  @Before
  def initializeRDD(): Unit ={
    rdd = sc.riakBucket[UserData](DEFAULT_NAMESPACE.getBucketNameAsString)
      .query2iRange(CREATION_INDEX, 1, 6)
  }

  @Test
  def calculateCount(): Unit = {
    val count = rdd.count()
    assertEquals(6, count) // scalastyle:ignore
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

  @Test
  def checkActions(): Unit ={
    val perUserTotalRDD = calculateUserOrderedTotals()
    val data = perUserTotalRDD.collect()
    assertEqualsUsingJSON("[['u1',3],['u2',1],['u3',2]]", data)
  }

  @Test
  def storePairRDDWithDefaultMapper(): Unit = {
    val perUserTotalRDD = calculateUserOrderedTotals()
    perUserTotalRDD.saveToRiak(DEFAULT_NAMESPACE_4STORE.getBucketNameAsString)

    // Read data from riak and populate data buffer
    val data =  ListBuffer[(String,Long)]()
    RiakConnector(sc.getConf).withSessionDo { session => {
      foreachKeyInBucket(session.unwrap(), DEFAULT_NAMESPACE_4STORE, (RiakConnector, l: Location) =>{
        val v = readByLocation[Long](session.unwrap(), l)
        data += ((l.getKeyAsString,v))
      })
    }}
    assertEquals(3, data.size)
  }
}
