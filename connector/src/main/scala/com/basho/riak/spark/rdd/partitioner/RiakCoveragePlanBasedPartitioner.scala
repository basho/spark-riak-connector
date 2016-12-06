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

import com.basho.riak.client.api.commands.kv.CoveragePlan.Builder
import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.query.QueryData
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.partitioner.PartitioningUtils._
import com.basho.riak.spark.rdd.{BucketDef, ReadConf, RiakPartition}
import org.apache.spark.{Logging, Partition}

import scala.collection.JavaConversions._
import scala.util.control.Exception._

case class RiakLocalCoveragePartition[K](
  index: Int,
  endpoints: Set[HostAndPort],
  primaryHost: HostAndPort,
  queryData: QueryData[K]) extends RiakPartition

object RiakCoveragePlanBasedPartitioner extends Logging {
  def partitions[K](connector: RiakConnector, bucket: BucketDef, readConf: ReadConf, queryData: QueryData[K]): Array[Partition] = {

    val partitionsCount = readConf.splitCount
    val coveragePlan = connector.withSessionDo { session =>

      val cmd = new Builder(bucket.asNamespace())
        .withMinPartitions(partitionsCount)
        .build()

      allCatch either session.execute(cmd) match {
        case Right(cp) => cp
        case Left(ex) if ex.getCause.isInstanceOf[RiakResponseException] && ex.getCause.getMessage.equals("Unknown message code: 70") =>
          throw new IllegalStateException("Full bucket read is not supported on your version of Riak", ex.getCause)
        case Left(ex) => throw ex
      }
    }

    // TODO: add proper Coverage Plan logging
    logTrace(s"Coverage plan:\n\t${coveragePlan.mkString("\n\t")}")

    val hosts = coveragePlan.hosts

    require(partitionsCount >= hosts.size, s"Requires $partitionsCount partitions but ${hosts.size()} hosts found")
    require(partitionsCount <= coveragePlan.size, s"Requires $partitionsCount partitions but coverage plan contains ${coveragePlan.size} partitions")

    val evenDistributionBetweenHosts = distributeEvenly(partitionsCount, hosts.size)
    val numberOfEntriesInPartitionPerHost = hosts
      .zip(evenDistributionBetweenHosts)
      .flatMap {
        case (h, num) => splitListEvenly(coveragePlan.hostEntries(h), num).map((h, _))
      }

    val partitions = numberOfEntriesInPartitionPerHost.zipWithIndex.map {
      case ((host, coverageEntries), partitionIdx) => new RiakLocalCoveragePartition(
        partitionIdx, hosts.toSet, host, queryData.copy(coverageEntries = Some(coverageEntries)))
    }
    partitions.toArray.sortBy(_.index).asInstanceOf[Array[Partition]]
  }
}
