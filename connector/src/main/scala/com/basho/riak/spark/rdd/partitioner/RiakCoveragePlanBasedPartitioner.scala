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
import com.basho.riak.spark.rdd.{RiakConnector, ReadConf, RiakPartition, BucketDef}
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
    connector.withSessionDo(session =>{
      val cmd = new Builder(bucket.asNamespace())
        .withMinPartitions(readConf.splitCount)
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

      val partitions = for {
        (ce, partitionIdx) <- coveragePlan.zipWithIndex
        partition = new RiakLocalCoveragePartition(partitionIdx,
            coveragePlan.hosts().toSet, HostAndPort.fromParts(ce.getHost, ce.getPort),
            queryData.copy(coverageEntries = Some(Seq(ce)))
          )
      } yield partition

      partitions.toArray
    })
  }
}
