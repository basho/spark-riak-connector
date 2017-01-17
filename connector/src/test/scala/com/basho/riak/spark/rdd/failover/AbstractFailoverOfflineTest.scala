package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.stub.{RiakMessageHandler, RiakNodeStub}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.riak.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.hamcrest.{Description, Matchers}
import org.junit.internal.matchers.ThrowableCauseMatcher
import org.junit.{After, Before}

import scala.collection.JavaConversions._

abstract class AbstractFailoverOfflineTest extends Logging {

  protected final val NAMESPACE = new Namespace("default", "test-bucket")
  protected final val COVERAGE_ENTRIES_COUNT = 64

  protected var sc: SparkContext = _
  protected var riakNodes: Seq[(HostAndPort, RiakNodeStub)] = _ // tuple HostAndPort -> stub

  val riakHosts: Int = 1

  val riakMessageHandler: Option[RiakMessageHandler] = None

  def sparkConf: SparkConf = new SparkConf(false)
    .setMaster("local")
    .setAppName(getClass.getSimpleName)
    .set("spark.riak.connection.host", riakNodes.map{case (hp, _) => s"${hp.getHost}:${hp.getPort}"}.mkString(","))
    .set("spark.riak.output.wquorum", "1")
    .set("spark.riak.input.fetch-size", "2")

  def initRiakNodes(): Seq[(HostAndPort, RiakNodeStub)] = {
    require(riakMessageHandler.isDefined)

    // start riak stubs on localhost and free random port
    (1 to riakHosts).map { _ =>
      val riakNode = RiakNodeStub(riakMessageHandler.get)
      riakNode.start() -> riakNode
    }
  }

  @Before
  def setUp(): Unit = {
    riakNodes = initRiakNodes()
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sc = sparkSession.sparkContext
  }

  @After
  def tearDown(): Unit = {
    Option(riakNodes).foreach(_.foreach(n => n._2.stop()))
    Option(sc).foreach(_.stop())
  }

  def distributeEvenly(size: Int, splitCount: Int): Seq[Int] = {
    val (base, rem) = (size / splitCount, size % splitCount)
    (0 until splitCount).map(i => if (i < rem) base + 1 else base)
  }
}

class RootCauseMatcher[T <: Throwable](val excClass: Class[T]) extends ThrowableCauseMatcher[T](Matchers.isA(excClass)) {

  private def getOneBeforeRootCause(item: T): Throwable = {
    val throwables = ExceptionUtils.getThrowableList(item)
    if (throwables.length > 1) {
      throwables.reverse.tail.head
    } else {
      throwables.head
    }
  }

  override def matchesSafely(item: T): Boolean = super.matchesSafely(getOneBeforeRootCause(item).asInstanceOf[T])

  override def describeMismatchSafely(item: T, description: Description): Unit =
    super.describeMismatchSafely(getOneBeforeRootCause(item).asInstanceOf[T], description)
}