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
package com.basho.spark.connector.rdd.partitioner

import com.basho.riak.client.api.commands.kv.CoveragePlan.Builder
import com.basho.riak.client.core.util.HostAndPort
import com.basho.spark.connector.query.RiakKeys
import com.basho.spark.connector.rdd.{ReadConf, BucketDef, RiakConnector, RiakPartition}
import org.apache.spark.Partition

import scala.collection.JavaConversions._

case class RiakLocalCoverPartition[K] (
    index: Int,
    endpoints: Set[HostAndPort],
    primaryHost: HostAndPort,
    queryData: RiakKeys[K]
) extends RiakPartition

object RiakLocalReadsPartitioner {
  def partitions[K](connector: RiakConnector, bucket: BucketDef, readConf: ReadConf, queryData: RiakKeys[K]): Array[Partition] = {
    connector.withSessionDo(session =>{
      val cmd = new Builder(bucket.asNamespace())
        //TODO: introduce Spark Conf parameter spark.riak.input.split.count
        .withMinPartitions(3)
        .build()

      val coveragePlan = session.execute(cmd)

      var partitionIdx = -1
      val partitions = for {
        ce <- coveragePlan
        partition = new RiakLocalCoverPartition({partitionIdx += 1; partitionIdx },
            coveragePlan.hosts().toSet, HostAndPort.fromParts(ce.getHost, ce.getPort),
            queryData.copy(coverageEntries = Some(Seq(ce)))
          )
      } yield partition

      partitions.toArray
    })
  }
}
