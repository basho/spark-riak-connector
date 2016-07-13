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

import com.basho.riak.spark._
import org.junit.experimental.categories.Category
import org.junit.{Assert, Test}

case class TSData(latitude: Float, longitude: Float, timestamp: String, user_id: String, gauge1: Int, gauge2: String)

@Category(Array(classOf[RiakCommonTests]))
class RDDTest extends AbstractRiakSparkTest {
  private final val CREATION_INDEX = "creationNo"

  protected override val jsonData = Some(
    """[
      | {key: 'my_key_1', indexes: {creationNo: 1}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 142}},
      | {key: 'my_key_2', indexes: {creationNo: 2}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 145, gauge2: 'min'}},
      |                  {indexes: {creationNo: 3}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 0}},
      |                  {indexes: {creationNo: 4}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 400, gauge2: '128'}},
      |                  {indexes: {creationNo: 5}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}},
      |                  {indexes: {creationNo: 6}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}},
      |                  {indexes: {creationNo: 7}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:06.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}},
      |                  {indexes: {creationNo: 8}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:07.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}},
      |                  {indexes: {creationNo: 9}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:06.823Z', user_id: '36e3616f-59f2-4096-b5cb-3ab94db5da41'}},
      |                  {indexes: {creationNo: 10}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:07.823Z', user_id: '36e3616f-59f2-4096-b5cb-3ab94db5da41'}}
      |]""".stripMargin)

  @Test
  def check2IPageableProcessing(): Unit = {
    val rdd = sc.riakBucket[Map[String, _]](DEFAULT_NAMESPACE).query2iRange(CREATION_INDEX, 1, 10) // scalastyle:ignore

    Assert.assertEquals(10, rdd.collect().length) // scalastyle:ignore
  }

  @Test
  def check2IRangeQuery(): Unit = {
    val rdd = sc.riakBucket[Map[String, _]](DEFAULT_NAMESPACE).query2iRange(CREATION_INDEX, 1, 2)

    assertEqualsUsingJSONIgnoreOrder(
      """[
        | {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 142},
        | {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 145, gauge2: 'min'}
        |]""".stripMargin, rdd.collect())
  }

  @Test
  def checkUDTMapping(): Unit = {
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE).query2iRange(CREATION_INDEX, 1, 2)

    assertEqualsUsingJSONIgnoreOrder(
      """[
        | {"latitude":28.946907,"longitude":-82.00319,"timestamp":"2014-11-24T13:14:04.823Z","user_id":"2c1421c4-161d-456b-a1c1-63ceededc3d5","gauge1":142,"gauge2":null},
        | {"latitude":28.946907,"longitude":-82.00319,"timestamp":"2014-11-24T13:14:05.823Z","user_id":"2c1421c4-161d-456b-a1c1-63ceededc3d5","gauge1":145,"gauge2":"min"}
        |]""".stripMargin, rdd.collect())
  }

  @Test
  def check2IRangeQueryPairRDD(): Unit = {
    val rdd = sc.riakBucket[(String, Map[String, _])](DEFAULT_NAMESPACE).query2iRange(CREATION_INDEX, 1, 2)

    assertEqualsUsingJSONIgnoreOrder(
      """[
        | ['my_key_1', {latitude: 28.946907,longitude: -82.00319,timestamp:'2014-11-24T13:14:04.823Z',user_id:'2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1:142}],
        | ['my_key_2', {latitude: 28.946907, longitude: -82.00319,timestamp: '2014-11-24T13:14:05.823Z',user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5',gauge1: 145,gauge2: 'min'}]
        |]""".stripMargin, rdd.collect())
  }

  @Test
  def checkDefaultDataMapping(): Unit = {
    val rdd = sc.riakBucket(DEFAULT_NAMESPACE).query2iRange(CREATION_INDEX, 1, 2)

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   ["my_key_2",{"gauge1":145,"timestamp":"2014-11-24T13:14:05.823Z","latitude":28.946907,"longitude":-82.00319,"user_id":"2c1421c4-161d-456b-a1c1-63ceededc3d5","gauge2":"min"}],
        |   ["my_key_1",{"gauge1":142,"timestamp":"2014-11-24T13:14:04.823Z","latitude":28.946907,"longitude":-82.00319,"user_id":"2c1421c4-161d-456b-a1c1-63ceededc3d5"}]
        |]""".stripMargin, rdd.collect())
  }
  
  @Test
  def test2iPartitioning(): Unit = {
    val size = 3
    val points = (1 to size + 1).map(_ * 100)
    val ranges = points zip points.tail
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
    .partitionBy2iRanges(CREATION_INDEX, ranges: _*)
    
    Assert.assertEquals(size, rdd.partitions.length)
  }
  
  @Test
  def test2iParallelizationIntMultiple(): Unit = {
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
    .query2iRange(CREATION_INDEX, 101, 2000)
    
    Assert.assertEquals(10, rdd.partitions.length)
  }
  
  @Test
  def test2iParallelizationBigIntMultiple(): Unit = {
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
    .query2iRange(CREATION_INDEX, BigInt(101L), BigInt(2000L))
    
    Assert.assertEquals(10, rdd.partitions.length)
  }
  
  @Test
  def test2iParallelizationLongMultiple(): Unit = {
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
    .query2iRange(CREATION_INDEX, 101L, 2000L)
    
    Assert.assertEquals(10, rdd.partitions.length)
  }
  
  @Test
  def test2iParallelizationIntSingle(): Unit = {
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
    .query2iRange(CREATION_INDEX, 101, 109)
    
    Assert.assertEquals(1, rdd.partitions.length)
  }

  @Test
  def test2iParallelizationStringShouldBeSingle(): Unit = {
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, "a", "z")

    Assert.assertEquals(1, rdd.partitions.length)
  }
}