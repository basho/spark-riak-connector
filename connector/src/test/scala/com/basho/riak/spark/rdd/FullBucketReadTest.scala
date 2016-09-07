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

import java.{lang => jl, util => ju}

import com.basho.riak.spark._
import com.basho.riak.test.rule.annotations.OverrideRiakClusterConfig
import org.apache.spark.SparkConf
import org.junit.Assert._
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

object FullBucketReadTest {

  @Parameters(name = "Split To {0} partitions" ) def parameters: ju.Collection[Array[jl.Integer]] = {
    val list = new ju.ArrayList[Array[jl.Integer]]()
    list.add(Array(12))
    list.add(Array(9))
    list.add(Array(6))
    list.add(Array(5))
    list.add(Array(3))
    list
  }
}

@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakTSTests]))
@OverrideRiakClusterConfig(nodes = 3, timeout = 5)
class FullBucketReadTest(splitSize: Int) extends AbstractRiakSparkTest {
  private val NUMBER_OF_TEST_VALUES = 1000

  protected override val jsonData = Some({
    val data = for {
      i <- 1 to NUMBER_OF_TEST_VALUES
      data = Map("key" -> s"k$i", "value" -> s"v$i", "indexes" -> Map("creationNo" -> i))
    } yield data

    asStrictJSON(data)
  })

  override protected def initSparkConf(): SparkConf = {
    val conf = super.initSparkConf()
    conf.set("spark.riak.input.split.count", splitSize.toString)
  }

  /*
     * map RDD[K] to RDD[(partitionIdx,K)]
     */
  val funcReMapWithPartitionIdx = new ((Int, Iterator[String]) => Iterator[(Int, String)]) with Serializable {
    override def apply(partitionIdx: Int, iter: Iterator[String]): Iterator[(Int, String)] = {
      iter.toList.map(x => partitionIdx -> x).iterator
    }
  }

  /**
   * Utilize CoveragePlan support to perform local reads
   */
  @Test
  def fullBucketRead(): Unit ={
      val data = sc.riakBucket[String](DEFAULT_NAMESPACE)
        .queryAll()
        .mapPartitionsWithIndex(funcReMapWithPartitionIdx, preservesPartitioning=true)
        .groupByKey()
        .collect()

      // verify split by partitions
      val numberOfPartitions = data.length
      assertEquals(splitSize, numberOfPartitions)

      // verify total number of values
      val allValues = data.flatMap{case (k,v)=> v}
        .sortBy(x=>x.substring(1).toLong )

      assertEquals(NUMBER_OF_TEST_VALUES, allValues.length)

      // verify returned values
      for{i <- 1 to NUMBER_OF_TEST_VALUES}{
        assertEquals( "v" + i, allValues(i - 1))
      }
  }
}
