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
import org.junit.Assert._
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[RiakTSTests], classOf[RiakBDPTests]))
class FullBucketReadTest extends AbstractRDDTest {
  private val NUMBER_OF_TEST_VALUES = 1000

  @Override
  protected override def jsonData(): String = {
    val data = for {
        i <- 1 to NUMBER_OF_TEST_VALUES
        data = Map("key"->s"k$i","value"->s"v$i", "indexes"->Map("creationNo"->i))
    } yield data

    asStrictJSON(data)
  }

  /*
   * map RDD[K] to RDD[(partitionIdx,K)]
   */
  val funcReMapWithPartitionIdx = new Function2[Int,Iterator[String], Iterator[(Int,String)]] with Serializable{
    override def apply(partitionIdx: Int, iter: Iterator[String]): Iterator[(Int, String)] = {
      iter.toList.map(x => partitionIdx -> x).iterator
    }
  }

  /**
   * Utilize CoveragePlan support to perform local reads
   */
  @Test
  def fullBucketRead() ={
    val data = sc.riakBucket[String](DEFAULT_NAMESPACE)
      .queryAll()
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx, preservesPartitioning=true)
      .groupByKey()
      .collect()

    // TODO: needs to verify number of created partitions
    val numberOfPartitions = data.size

    val allValues = data.map{case (k,v)=> v}
      .flatten
      .sortBy(x=>x.substring(1).toLong )

    assertEquals(NUMBER_OF_TEST_VALUES, allValues.size)

    // verify returned values
    for(i <- 1 to NUMBER_OF_TEST_VALUES){
      assertEquals( "v"+i, allValues(i-1))
    }
  }
}
