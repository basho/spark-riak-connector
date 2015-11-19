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
package com.basho.riak.spark.rdd.partitioner

import java.util.concurrent.ExecutionException

import com.basho.riak.client.api.commands.kv.CoveragePlan
import com.basho.riak.client.api.commands.kv.CoveragePlan.Builder
import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.query.QueryData
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.{BucketDef, ReadConf, RiakPartition}
import org.apache.spark.Partition

import scala.collection.JavaConversions._

case class RiakLocalCoveragePartition[K] (
    index: Int,
    endpoints: Set[HostAndPort],
    primaryHost: HostAndPort,
    queryData: QueryData[K]
) extends RiakPartition

/**
 * Obtains Coverage Plan and creates a separate partition for each Coverage Entry
 */
object RiakCoveragePlanBasedPartitioner {
  def partitions[K](connector: RiakConnector, bucket: BucketDef, readConf: ReadConf, queryData: QueryData[K]): Array[Partition] = {


    def splitListEvenly[A](list: Seq[A], splitCount: Int) = {
    val (base, rem) =  divide(list.size, splitCount)
      val (smaller, bigger) = list.splitAt(list.size - rem * (base + 1))
      smaller.grouped(base) ++ bigger.grouped(base + 1)
    }

    // e.g. split 64 coverage entries into 10 partitions: (6,6,6,6,6,6,7,7,7,7) coverage entries in partitions respectively
    def distributeEvenly(size: Int, splitCount: Int): Seq[Int] = {
      val (base, rem) =  divide(size, splitCount)
      for(i <- 0 until splitCount)
         yield if(i < rem) base + 1 else base
    }

    def divide(size: Int, splitCount: Int) = {
      (size / splitCount, size % splitCount)
    }

    connector.withSessionDo(session =>{
      val partitionsCount = readConf.splitCount

      val cmd = new Builder(bucket.asNamespace())
        .withMinPartitions(partitionsCount)
        .build()

      var coveragePlan: CoveragePlan.Response = null
      try {
        coveragePlan = session.execute(cmd)
      } catch {
        case e: ExecutionException =>
          if (e.getCause.isInstanceOf[RiakResponseException] && e.getCause.getMessage.equals("Unknown message code: 70")) {
            throw new IllegalStateException("Full bucket read is not supported on your version of Riak", e.getCause)
          } else throw e
      }

      // TODO: add proper Coverage Plan logging

      val hosts = coveragePlan.hosts

      require(partitionsCount >= hosts.size)
      require(partitionsCount <= coveragePlan.size)

      val coverageEntriesCount = coveragePlan.size

      val evenDistributionBetweenHosts = distributeEvenly(partitionsCount, hosts.size)
      evenDistributionBetweenHosts.foreach(println(_))
      val numberOfEntriesInPartitionPerHost =
        (hosts zip evenDistributionBetweenHosts).flatMap { case (h, num) => splitListEvenly(coveragePlan.hostEntries(h), num).map((h, _)) }

      val partitions = for {
        ((host, coverageEntries), partitionIdx) <- numberOfEntriesInPartitionPerHost.zipWithIndex
        partition = new RiakLocalCoveragePartition(partitionIdx,
          hosts.toSet, host,
          queryData.copy(coverageEntries = Some(coverageEntries)))
      } yield partition

      partitions.toArray
    })
  }
}
