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
        s"                        Actual: the only $partitionsCount partitions will be created\n" +
        "--\n" +
        s"Coverage plan:\n" +
        DumpUtils.dumpWithIdx(coveragePlan, "\n  ") +
        "\n----------------------------------------\n")
    }

    val hosts = coveragePlan.hosts

    require(partitionsCount >= hosts.size, s"Requires $partitionsCount partitions but ${hosts.size()} hosts found")
    require(partitionsCount <= coveragePlan.size, s"Requires $partitionsCount partitions but coverage plan contains ${coveragePlan.size} partitions")


    /**
      * Spark Partition distribution across the Riak hosts, sorted in a reverse order to preserve bigger splits on top.
      * Each value represents a number of partitions that should be created.
      */
    val partitionDistribution: Seq[Int] = distributeEvenly(partitionsCount, hosts.size).sorted.reverse

    require( partitionsCount == partitionDistribution.foldLeft(0)(_ + _))

    /**
      * Per host coverage entries, sorted to preserve hosts that have more coverage entries on top
      */
    val perHostCoverageEntries = ListMap(
      (for {
        h <- hosts
        perHost = h -> coveragePlan.hostEntries(h)
      } yield perHost)
        // sort by number of coverage entries, bigger sequence wins
        .toSeq.sortWith(_._2.size() > _._2.size()): _*)

    /**
      * Flat sequence of host -> Seq[CoverageEntry] tuples, created according to the partition distribution,
      * each tuple represents one partition.
      *
      * Note: as soon as both perHostCoverageEntries and partitionDistribution are sorted to preserve on top a bigger split
      * and a bigger coverage entries sequence accordingly, the chances are good to met the distribution and split
      * the biggest sequence into the biggest number of chunks.
      */
    val riakPartitionData =
      (perHostCoverageEntries.keys zip partitionDistribution) flatMap {
        case (h: HostAndPort, num: Int) =>
          /**
            * split list of host coverage entries into num parts (whereas num is the number of Spark partitions)
            * and map'em to the host -> Seq[CoverageEntry] tuples
            */
          splitListEvenly(perHostCoverageEntries.get(h).get, num) map  {h ->  _}
      }

    val partitions = riakPartitionData.zipWithIndex.map {
      case ((host: HostAndPort, coverageEntries: Seq[_]), partitionIdx) => new RiakLocalCoveragePartition(
        partitionIdx, hosts.toSet, host, queryData.copy(coverageEntries = Some(coverageEntries)))
    }

    val result = partitions.toArray.sortBy(_.index)

    if (log.isDebugEnabled()) {
      logDebug("\n----------------------------------------\n" +
        s" [Auto KV Partitioner]  Requested: split up to $splitCount partitions\n" +
        s"                        Actual: ${result.length} partitions have been created\n" +
        "--\n" +
        DumpUtils.dump(result, "\n") +
        "\n----------------------------------------\n")
    }

    // Double check that all coverage entries were used
    val numberOfUsedCoverageEntries = partitions.foldLeft(0){ (sum, p) => sum + p.queryData.coverageEntries.getOrElse(Seq.empty).size}

    if (numberOfUsedCoverageEntries != coverageEntriesCount) {
      logError("\n----------------------------------------\n" +
        "  [ERROR] Some coverage entries do not belong to any spark partition\n" +
        "--\n" +
        " Coverage Plan:\n  " +
        DumpUtils.dumpWithIdx(coveragePlan, "\n  ") +
        "\n--\n" +
        " Created partitions:" +
        DumpUtils.dump(result, "\n") +
        "\n----------------------------------------\n")

      throw new IllegalStateException("Some coverage entries do not belong to any of the created spark " +
        "partitions. This will lead to an incomplete data load, see log for more details.")
    }


    result.asInstanceOf[Array[Partition]]
  }
}
