/**
  * Copyright (c) 2015-2017 Basho Technologies, Inc.
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
import com.basho.riak.spark.util.DumpUtils
import org.apache.spark.{Logging, Partition, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.util.control.Exception._

case class RiakLocalCoveragePartition[K](
                                          index: Int,
                                          endpoints: Set[HostAndPort],
                                          primaryHost: HostAndPort,
                                          queryData: QueryData[K]) extends RiakPartition {

  override def dump(lineSep: String = "\n"): String =
    s"[$index] eps: " + DumpUtils.dump(endpoints, ", ") + lineSep +
      "   primaryHost: " + DumpUtils.dump(primaryHost) + lineSep +
      "   queryData:" + lineSep +
      "      " + queryData.dump(lineSep + "      ")
}

object RiakCoveragePlanBasedPartitioner extends Logging {
  def partitions[K](sc: SparkContext, connector: RiakConnector, bucket: BucketDef, readConf: ReadConf, queryData: QueryData[K]): Array[Partition] = {
    val partitionsCount = readConf.getOrSmartSplitCount(sc)
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

    val splitCount = readConf.getOrDefaultSplitCount(ReadConf.DefaultSplitCount)

    val coverageEntriesCount = coveragePlan.size
    if (log.isTraceEnabled()) {
      logTrace("\n----------------------------------------\n" +
        s" [Auto TS Partitioner]  Requested: split up to $splitCount partitions\n" +
        s"                        Actually: the only $partitionsCount partitions might be created\n" +
        "--\n" +
        s"Coverage plan:\n" +
        DumpUtils.dumpWithIdx(coveragePlan, "\n  ") +
        "\n----------------------------------------\n")
    }

    val hosts = coveragePlan.hosts

    require(partitionsCount >= hosts.size, s"Requires $partitionsCount partitions but ${hosts.size()} hosts found")
    require(partitionsCount <= coveragePlan.size, s"Requires $partitionsCount partitions but coverage plan contains ${coveragePlan.size} partitions")


    val evenPartitionDistributionBetweenHosts = distributeEvenly(partitionsCount, hosts.size).sorted.reverse
    require( partitionsCount == evenPartitionDistributionBetweenHosts.foldLeft(0)(_ + _))

    val sortedCoverageEntries = ListMap(
      (for {
        h <- hosts
        perHost = h -> coveragePlan.hostEntries(h)
      } yield perHost)
        .toSeq.sortWith(_._2.size() > _._2.size()): _*)

    val numberOfEntriesInPartitionPerHost =
      (sortedCoverageEntries.keys zip evenPartitionDistributionBetweenHosts) flatMap {
        case (h, num) =>
          val spl = splitListEvenly(coveragePlan.hostEntries(h), num)
          spl map {(h-> _)}
      }

    val partitions = for {
      ((host, coverageEntries), partitionIdx) <- numberOfEntriesInPartitionPerHost.zipWithIndex
      partition = new RiakLocalCoveragePartition(
        partitionIdx, hosts.toSet, host, queryData.copy(coverageEntries = Some(coverageEntries)))
    } yield partition

    val result = partitions.toArray.sortBy(_.index)

    if (log.isDebugEnabled()) {
      logDebug("\n----------------------------------------\n" +
        s" [Auto KV Partitioner]  Requested: split up to $splitCount partitions\n" +
        s"                        Actually: ${result.length} partitions have been created\n" +
        "--\n" +
        DumpUtils.dump(result, "\n") +
        "\n----------------------------------------\n")
    }

    // Double check that all coverage entries were used
    val numberOfUsedCoverageEntries = partitions.foldLeft(0){ (sum, p) => sum + p.queryData.coverageEntries.getOrElse(Seq.empty).size}
    require( numberOfUsedCoverageEntries == coverageEntriesCount)

    result.asInstanceOf[Array[Partition]]
  }
}
