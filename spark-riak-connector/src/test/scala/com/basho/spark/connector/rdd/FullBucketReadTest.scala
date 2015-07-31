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
package com.basho.spark.connector.rdd

import org.junit.{Ignore, Test}
import com.basho.spark.connector._
import org.junit.Assert._

class FullBucketReadTest extends AbstractRDDTest {
  private val NUMBER_OF_TEST_VALUES = 20

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

    val count = data.size

    // TODO: needs to verify the number of the created partitions
    val allValues = data.map{case (k,v)=> v}.flatten
    assertEquals(NUMBER_OF_TEST_VALUES, allValues.size)
  }
}
