package com.basho.riak.spark.examples.demos.fbl
import java.util.Calendar
import java.util.concurrent.{Semaphore, TimeUnit}

import com.basho.riak.client.api.commands.kv.StoreValue
import com.basho.riak.client.api.commands.kv.StoreValue.Response
import com.basho.riak.client.core.query.indexes.LongIntIndex
import com.basho.riak.client.core.{RiakFuture, RiakFutureListener}
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.slf4j.{Logger, LoggerFactory}
import java.util.zip.ZipInputStream

import com.basho.riak.client.core.query.Location
import com.basho.riak.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import com.basho.riak.spark.rdd.RiakFunctions
import com.basho.riak.spark._
import org.apache.spark.sql.SparkSession

object FootballDemo {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val FBL_SOURCE_DATA = "fbl-data"
  private val FBL_TEAM_WINS_AND_LOSS = "fbl-team-wins-and-loss"
  private val FBL_TEAM_HISTORY = "fbl-team-score-history"
  private val FBL_TEAM_SCORES = "fbl-team-scores"
  private val FBL_BEST_QTRS = "fbl-best-quarter"

  val CFG_DEFAULT_BUCKET = FBL_SOURCE_DATA
  val CFG_DEFAULT_FROM = 2002L
  val CFG_DEFAULT_TO = Calendar.getInstance().get(Calendar.YEAR).toLong
  val CFG_DEFAULT_INDEX = "season"

  val NUMBER_OF_PARALLEL_REQUESTS = 10
  //scalastyle:off
  def main(args: Array[String]): Unit = {

    val usage = "FootballDemo usage: available options are:\n" +
      "  [--truncate-data]  all Riak buckets used for this demo will be truncated and no calculations will be performed\n" +
      "  [--help]  displays usage information"

    var theOnlyTruncateRequired = false

    // -- simple CLI
    if(args.length > 1){
      println(usage)
      sys.exit(1)
    } else if(args.length == 1) {
      args(0) match {
        case "--truncate-data" =>
          theOnlyTruncateRequired = true
          println("All Riak buckets used for this demo will be truncated and no calculations will be performed")
        case "--help" =>
          println(usage)
          sys.exit(0)
        case _ =>
          println("Unknown option "+ args(0))
          println(usage)
          sys.exit(1)
      }
    }

    // --
    val sparkConf = new SparkConf()
      .setAppName("Football Demo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Now it's 24 Mb of buffer by default instead of 0.064 Mb
      //.set("spark.kryoserializer.buffer.mb","24")
      .set("spark.kryoserializer.buffer","24m")

    // -- Apply defaults to sparkConf if the corresponding value doesn't set
    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf,"spark.executor.memory", "4g")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:10017")
    setSparkOpt(sparkConf, "spark.riak.demo.index", CFG_DEFAULT_INDEX)
    setSparkOpt(sparkConf, "spark.riak.demo.bucket", CFG_DEFAULT_BUCKET)
    setSparkOpt(sparkConf,"spark.riak.demo.from", CFG_DEFAULT_FROM.toString)
    setSparkOpt(sparkConf,"spark.riak.demo.to", CFG_DEFAULT_TO.toString)


    val demoCfg = Demo2iConfig(sparkConf)

    // -- Cleanup
    val rf = RiakFunctions( demoCfg.riakConf.hosts, NUMBER_OF_PARALLEL_REQUESTS)

    for(ns <-List(FBL_TEAM_WINS_AND_LOSS, FBL_TEAM_HISTORY, FBL_TEAM_SCORES, FBL_BEST_QTRS)) {
      rf.resetAndEmptyBucketByName(ns)
    }

    if(theOnlyTruncateRequired){
      rf.resetAndEmptyBucketByName(FBL_SOURCE_DATA)
      sys.exit()
    }

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    // -- Create test data
    createTestData(sc)

    // -- Perform calculations
    execute(sc, demoCfg.from, demoCfg.to, demoCfg.index, demoCfg.bucket)
  }

  def execute(sc: SparkContext, from: Long = CFG_DEFAULT_FROM, to: Long = CFG_DEFAULT_TO,
              index: String = CFG_DEFAULT_INDEX, bucket: String = FBL_SOURCE_DATA) = {

    println(s"Football stats calculated for $index from $from to $to")

    // Let's skip all events that don't have 'off' value at all, if any
    val rdd = sc.riakBucket[(String, List[Map[String, _]])](bucket, "default")
      .query2iRange(index, from, to)
      .mapValues( x => x.filterNot( _.getOrElse("off", "").toString.isEmpty ))
      .filter(f=> f._2.nonEmpty)

    rdd.cache()

    // -- Totals
    println("\nHow many games do we have in total?")
    val gamesInTotal = rdd.count()
    print(s"\t$gamesInTotal")

    // -- Seasons
    val seasons = rdd.keys.map(k=>k.substring(0,4))
      .distinct()
      .sortBy(c=>c,true)
      .collect()

    println(s"\nWhich seasons does this dataset contain?\n\t${seasons.deep.mkString(",")}")

    // ----------  MORE SOPHISTICATED STATS

    // RDD contains the last event of each game
    val game_end = rdd.map( g => g._2.last)

    // -- How many times did different teams win and lose
    // Find the last event of each game and extract the winner and loser by the final score

    // winner->loser rdd
    val winnerLoser = game_end.map( g => {
      val oscore = g.get("offscore").get.asInstanceOf[Int]
      val dscore = g.get("defscore").get.asInstanceOf[Int]
      val defTeam = g.getOrElse("def", "unknown")
      val offTeam = g.getOrElse("off", "unknown")

      oscore - dscore match {
        case x: Int if x > 0 =>
          offTeam -> defTeam
        case _ =>
          defTeam -> offTeam
      }
    })

    val winners = winnerLoser.map( x => (x._1,1L)).reduceByKey(_ + _)
    val losers = winnerLoser.map(x => (x._2, 1L)).reduceByKey(_ + _)
    val teamsWinsAndLosses = winners.join(losers)
    println(s"\nWhat were team records (win & loss) for the period?\n\t${teamsWinsAndLosses.collect().deep.mkString("\n\t")}")

    // Save team wins & loss to Riak bucket
    teamsWinsAndLosses.saveToRiak(FBL_TEAM_WINS_AND_LOSS)

    // -- How did the game score for each team changed since 2002?
    val teamScoreHistory = game_end.flatMap( g=> {
      val oscore = g.get("offscore").get
      val dscore = g.get("defscore").get
      val defTeam = g.getOrElse("def", "unknown")
      val offTeam = g.getOrElse("off", "unknown")

      val date = g.get("gameid").get.toString.substring(0,8).toInt
      List(offTeam -> (oscore, date), defTeam -> (dscore, date))
    }).groupByKey()

    val score = teamScoreHistory.map( x=> (x._1, x._2.toList.sortBy(r => r._2)))
    val d = score.take(1)
    println(s"How did the game score for each team changed since 2002?\n\t${score.take(10).deep.mkString("\n\t")}")

    // Save scores to Riak
    teamScoreHistory.saveToRiak(FBL_TEAM_HISTORY)

    // -- TOP 5 teams, that scored most
    val offscore = game_end.map(g => (g.getOrElse("off", "unknown"), g.getOrElse("offscore", "0").toString.toInt)).reduceByKey(_ + _)
    val defscore = game_end.map(g => (g.getOrElse("def", "unknown"), g.getOrElse("defscore", "0").toString.toInt)).reduceByKey(_ + _)

    val teamScore = offscore.join(defscore).map(t => (t._1, t._2._1 + t._2._2))
        .sortBy(t => t._2, ascending = false)

    println(s"Top 5 Teams, ranked by score\n\t${teamScore.take(5).deep.mkString("\n\t")}")

    // Save top5 teams to Riak
    teamScore.saveToRiak(FBL_TEAM_SCORES)

    // -- Which quarter is the most effective for different teams?
    val qtr = rdd.flatMap(g => {
      g._2.map(x => {
        ((g._1, x.getOrElse("qtr", "NA").toString, x.getOrElse("off", "unk")), x.getOrElse("offscore", "NA").toString)
      })
    }).filter(x => x._2 != "NA" && x._1._2 != "NA" && x._1._2 != "5").groupByKey()

    val qtr_score = qtr.mapValues(x => x.toList.last.toInt - x.toList.head.toInt).map(x => ((x._1._3, x._1._2), x._2))
    val avg_qtr_score = qtr_score.mapValues((_, 1)).reduceByKey((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    }).mapValues(x => x._1.toDouble / x._2.toDouble)

    val best_qtr = avg_qtr_score.map(x => (x._1._1, (x._1._2, x._2))).reduceByKey((x,y) => (x._1, List(x._2, y._2).max))

    println(s"Which quarter is the most effective for each team?\n\t${best_qtr.take(10).deep.mkString("\n\t")}")

    // Save best quarter to riak
    best_qtr.saveToRiak(FBL_BEST_QTRS)
  }

  def createTestData(sc: SparkContext): Unit = {
    val demoConfig = Demo2iConfig(sc.getConf)
    println(s"Test data creation for ${demoConfig.name}, '${demoConfig.bucket}' will be truncated")

    val rf = RiakFunctions( demoConfig.riakConf.hosts, NUMBER_OF_PARALLEL_REQUESTS)

    /**
     * Before creating test data we need to be sure that bucket is empty,
     * therefore we need to purge all data from it
      */
    rf.resetAndEmptyBucketByName(demoConfig.bucket)

    val is = this.getClass.getResourceAsStream("/fbl-data.zip")
    val zs = new ZipInputStream(is)

    try {
      var ze = zs.getNextEntry
      while (ze != null) {

        val fileName = ze.getName

        logger.info(s"Start processing '$fileName'")
        val src = Source.fromInputStream(zs).getLines()

        // assuming first line is a header
        val header = src.take(1).next.split(",", -1)
        require( header.nonEmpty, "source csv file must have a header")

        val games = mutable.Map[String, mutable.ListBuffer[mutable.Map[String,_]]]()
        for(l <- src) {
          val r = l.split(",", -1)
          val gameid = r(0)
          val event = mutable.Map[String,Any]( (header zip r).toMap.toSeq: _* )

          event.put("offscore", valueAsInt(event.get("offscore")))
          event.put("defscore", valueAsInt(event.get("defscore")))

          games.get(gameid) match {
            case Some(l) => l += event
            case None => games.put(gameid, ListBuffer(event))
          }
        }

        rf.withRiakDo(session => {
          val semaphore = new Semaphore(NUMBER_OF_PARALLEL_REQUESTS)
          val creationListener = new RiakFutureListener[StoreValue.Response, Location] {
            override def handle(f: RiakFuture[Response, Location]): Unit = {
              try {
                f.get()
              } finally {
                semaphore.release()
              }
            }
          }

          // Process all not empty events (events which have non empty gameid and at least 1 event)
          for ((gameid, events) <- games if !gameid.isEmpty && events.nonEmpty){
            val season = gameid.substring(0,4).toLong

            val ro = RiakObjectConversionUtil.to(events)

            // TODO: change ugly syntax
            ro.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named(demoConfig.index))
              .add(season)

            semaphore.acquire()
            rf.createValueAsyncForBucket(session, demoConfig.bucket, ro, gameid)
              .addListener(creationListener)
          }

          // wait until completion of all already started operations
          try {
            semaphore.tryAcquire(NUMBER_OF_PARALLEL_REQUESTS, 30, TimeUnit.SECONDS)
            semaphore.release(NUMBER_OF_PARALLEL_REQUESTS)
          }catch {
            case e: InterruptedException => logger.error("Not all operations were done")
              throw e
          }
        })
        ze = zs.getNextEntry
      }
    }finally {
      zs.close()
    }
  }

  private def valueAsInt(values: Option[_]): Int = values match {
    case Some(x) => x match {
      case x: String =>
        val v = x.trim
        if (v.isEmpty) {
          0
        } else {
          v.toInt
        }
      case _ => throw new RuntimeException("Only string converter is implemented")
    }

    case None => 0
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
