package com.basho.riak.spark.rdd.failover

import com.basho.riak.spark.rdd.{FullBucketReadTest, RiakTSTests}
import com.basho.riak.stub.{ProxyMessageHandler, RiakNodeStub}
import org.junit.After
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakTSTests]))
class RiakStubProxyTest(splitSize: Int) extends FullBucketReadTest(splitSize) {

  var stubNode: RiakNodeStub = _

  override def initialize(): Unit = {
    super.initialize()
    stubNode = RiakNodeStub(new ProxyMessageHandler(riakHosts.toList.head))
    stubNode.start()
  }

  @After
  override def destroySparkContext(): Unit = {
    stubNode.stop()
    super.destroySparkContext()
  }
}