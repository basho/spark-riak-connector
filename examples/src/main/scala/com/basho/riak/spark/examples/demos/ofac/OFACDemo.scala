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
package com.basho.riak.spark.examples.demos.ofac

import com.basho.riak.client.core.query.indexes.LongIntIndex
import com.basho.riak.spark.rdd.{BucketDef, RiakFunctions}
import com.basho.riak.spark.util.RiakObjectConversionUtil
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.annotation.meta.field
import com.basho.riak.spark._
import com.basho.riak.client.core.query.{Namespace, RiakObject}
import com.basho.riak.client.api.annotations.{RiakIndex, RiakKey}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object OFACDemo {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var stopwords = Array("")

  private val OFAC_SOURCE_DATA = "OFAC-data"
  private val OFAC_COUNTRY_BANS = "OFAC-country-bans"
  private val OFAC_VESSTYPE_BANS = "OFAC-vessel-type-bans"
  private val OFAC_TONNAGE_HIST = "OFAC-tonnage-hist"
  private val OFAC_TITLES = "OFAC-titles"

  val CFG_DEFAULT_BUCKET = OFAC_SOURCE_DATA
  val CFG_DEFAULT_FROM = 1
  val CFG_DEFAULT_TO = 20000.toLong
  val CFG_DEFAULT_INDEX = "entNum"

  val NUMBER_OF_PARALLEL_REQUESTS = 10

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("OFAC Demo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Now it's 24 Mb of buffer by default instead of 0.064 Mb
      .set("spark.kryoserializer.buffer","24")

    // -- Apply defaults to sparkConf if the corresponding value doesn't set
    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf,"spark.executor.memory", "512M")

    //setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:10017")
    setSparkOpt(sparkConf, "spark.riak.connections.min", NUMBER_OF_PARALLEL_REQUESTS.toString)
    setSparkOpt(sparkConf, "spark.riak.connections.max", (NUMBER_OF_PARALLEL_REQUESTS * 3).toString)

    setSparkOpt(sparkConf, "spark.riak.demo.index", CFG_DEFAULT_INDEX)
    setSparkOpt(sparkConf, "spark.riak.demo.bucket", CFG_DEFAULT_BUCKET)
    setSparkOpt(sparkConf,"spark.riak.demo.from", CFG_DEFAULT_FROM.toString)
    setSparkOpt(sparkConf,"spark.riak.demo.to", CFG_DEFAULT_TO.toString)

    // -- Create spark context
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    // -- Cleanup Riak buckets before we start
    val rf = RiakFunctions(sparkConf)
    for(buckeetName <-List(OFAC_VESSTYPE_BANS, OFAC_COUNTRY_BANS, OFAC_TONNAGE_HIST, OFAC_TITLES)) {
      rf.resetAndEmptyBucketByName(buckeetName)
    }

    // -- Create test data
    createTestData(sc)

      // -- Perform calculations
    execute(sc, CFG_DEFAULT_FROM, CFG_DEFAULT_TO, CFG_DEFAULT_INDEX, CFG_DEFAULT_BUCKET)
  }

  def execute(sc: SparkContext, from: Long = CFG_DEFAULT_FROM, to: Long = CFG_DEFAULT_TO,
              index: String = CFG_DEFAULT_INDEX, bucket: String = OFAC_SOURCE_DATA) = {

    println(s"OFAC stats will be calculated for $index from $from to $to")

    val rdd = sc.riakBucket[(String, Map[String, _])](bucket).query2iRange(index, from, to).map(x => {
      x._2("data").asInstanceOf[Map[String,String]]
    })

    rdd.cache() // We'll use this RDD many times

    // ----------  Descriptive Statistics
    // -- Totals
    val total = rdd.count()
    println(s"\nHow many unique entries do we have in total?\n\t$total")

    // -- Specially Designated Nationals (SDN) types
    val sdnTypes = rdd.filter(_.get("SDN_Type").get != "-0-").map(_.get("SDN_Type").get).distinct().collect()
    println(s"\nWhich SDN types does this dataset contain?\n\t${sdnTypes.deep.mkString(",")}")

    // ----------  Exploratory Statistics
    // -- Let's find out the number of banned individuals by each country
    // '-0-' means NA value in the OFAC list, so we need to filter it out first
    val cntr = rdd.filter(x => {
      !x.get("Country").get.toString.contains("-0-") && x.get("SDN_Type").get.toString.contains("individual")
    }).map(x => (x.get("SDN_Name").get, x.get("Country").get)).distinct().map(x => (x._2, 1)).countByKey()

    val countryBans = sc.parallelize(cntr.toList.sortBy(-_._2))
    println(s"\nLet's find out the number of banned individuals by each country:\n\t${countryBans.take(20).mkString("\n\t")}")

    // Save to Riak
    countryBans.saveToRiak(OFAC_COUNTRY_BANS)

    // -- How many vessels are in the OFAC list by country and vessel type?
    val vess = rdd.filter(x => {
      !x.get("Vess_type").get.toString.contains("-0-") && !x.get("Vess_flag").get.toString.contains("-0-")
    }).map(x => {
      val flag = x.get("Vess_flag").get.toString.toLowerCase
      val vesselType = x.get("Vess_type").get.toString
      ((flag, vesselType), 1)
    })

    val vessTypeBans = sc.parallelize(vess.countByKey().toList)
    println(s"\nHow many vessels are in the OFAC list by country and vessel type?\n\t${vessTypeBans.take(20).toMap.mkString("\n\t")}")

    // Save to Riak
    vessTypeBans.saveToRiak(OFAC_VESSTYPE_BANS)

    // -- Build a histogram of the vessel's tonnage
    val bins = 20
    val tonnage = rdd.filter(!_.get("Tonnage").get.toString.contains("-0-")).map(x => {
      x.get("Tonnage").get.toString.replaceAll("[,\"]", "").toDouble
    })

    val tonnageHist = tonnage.histogram(bins)
    val RangesAndCount = sc.parallelize((tonnageHist._1 zip tonnageHist._1.tail) zip tonnageHist._2)
    println(s"\nBuild a histogram of the vessel tonnage:\n\t${RangesAndCount.take(20).mkString("\n\t")}")

    // Save to Riak
    RangesAndCount.saveToRiak(OFAC_TONNAGE_HIST)

    // -- What kind of titles do individuals in the OFAC list use?
    val titles = rdd.filter(x => {
      !x.get("Title").get.toString.contains("-0-") && x.get("SDN_Type").get.toString.contains("individual")
    }).flatMap(_.get("Title").get.toString.toLowerCase.replaceAll("[,\"]", "").split(" ").map((_, 1)))

    // Remove frequent words and count frequency of the remaining
    val wordcount = titles.filter(w => !stopwords.contains(w._1)).reduceByKey(_ + _).filter(_._2.toInt > 5).sortBy(-_._2)
      .map(w => (w._1.toString.capitalize, w._2))
    println(s"\nWhat kind of titles do individuals in the OFAC list use?\n\t${wordcount.take(20).mkString("\n\t")}")

    wordcount.saveToRiak(OFAC_TITLES)
  }

  def createTestData(sc: SparkContext): Unit = {
    val rf = RiakFunctions(sc.getConf)
    rf.resetAndEmptyBucketByName(OFAC_SOURCE_DATA)

    println(s"Test data creation for OFAC-Demo")

    val txt = Source.fromURL("http://algs4.cs.princeton.edu/35applications/stopwords.txt").mkString
    stopwords = txt.split("\n").map(_.trim)

    val sdnHeader = List("ent_num", "SDN_Name","SDN_Type","Program","Title","Call_Sign","Vess_type","Tonnage","GRT","Vess_flag","Vess_owner","Remarks")
    val addHeader = List("ent_num", "Add_num", "Address","City_State_ZIP","Country", "Add_remarks")

    // Read SDN.CSV file into RDD
    val sdn_file = Source.fromURL("https://www.treasury.gov/ofac/downloads/sdn.csv").getLines.map(_.trim).toSeq
    val sdn = sc.parallelize(sdn_file).map(x => {
      val row = x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.replace("\"","").trim)
      (row(0), sdnHeader.zip(row))
    }).filter(_._1 != "")

    // Read ADD.CSV file into RDD
    val addr_file = Source.fromURL("https://www.treasury.gov/ofac/downloads/add.csv").getLines.map(_.trim).toSeq
    val addr = sc.parallelize(addr_file).map(x => {
      val row = x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.replace("\"","").trim)
      (row(0), addHeader.tail.zip(row.tail))
    }).filter(_._1 != "")

    // Create RDD content object with Riak secondary index suitable for this demo
    val rdd = sdn.leftOuterJoin(addr).map(x => (x._1, List(x._2._1, x._2._2.get).flatten.toMap)).map(x => {
      new DemoRiakRecord(key=x._1.trim, index = x._1.toLong, x._2)
    }).cache()

    if(logger.isDebugEnabled()) {

      val qntEmptyData = rdd.filter(x => x.data.isEmpty).count()
      val qntNULLKeys = rdd.filter(x => List(x.data.keys).isEmpty).count()
      val firstEl = rdd.first()

      logger.debug("Source data statistics:\n" +
          s"Total number of records: ${rdd.count}\n" +
            s"\tnumber of elements with empty data: $qntEmptyData\n" +
            s"\tnumber of elements with zero keys in Map: $qntNULLKeys\n" +
            "The first value is:\n" +
            rf.asStrictJSON(firstEl, true)
        )
    }

    implicit val vwf = new WriteDataMapperFactory[DemoRiakRecord, (String, RiakObject)] {
      override def dataMapper(bucket: BucketDef): WriteDataMapper[DemoRiakRecord, (String, RiakObject)] = {
        new WriteDataMapper[DemoRiakRecord, (String, RiakObject)] {
          override def mapValue(value: DemoRiakRecord): (String, RiakObject) = {
            val ro = RiakObjectConversionUtil.to(value)
            ro.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named("entNum"))
              .add(value.index)
            (value.key, ro)
          }
        }
      }
    }

    // Store test data into riak bucket
    rdd.saveToRiak(CFG_DEFAULT_BUCKET)
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }

  case class DemoRiakRecord(
    @(RiakKey@field)
    key: String,

    @(RiakIndex@field)(name = "entNum")
    index: Long,

    data: Map[String, String]
  )
}

