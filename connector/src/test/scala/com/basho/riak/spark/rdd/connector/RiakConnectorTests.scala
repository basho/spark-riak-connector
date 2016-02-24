package com.basho.riak.spark.rdd.connector

import java.lang.reflect.{InvocationHandler, Method}

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.util.HostAndPort
import org.apache.commons.lang3.reflect.FieldUtils
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Before, Test}
import org.mockito.Mockito.{never, only, verify}
import org.powermock.api.mockito.PowerMockito._
import org.powermock.api.support.membermodification.MemberMatcher._
import org.powermock.api.support.membermodification.MemberModifier.replace
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner

import scala.collection.JavaConversions._

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[SessionCache]))
class RiakConnectorTests {

  private val conf = RiakConnectorConf(HostAndPort.hostsFromString("1.1.1.1", 8087).toSet, 1, 2, 1000)
  private val connector = new RiakConnector(conf)

  @Before
  def purgeSessionCache(): Unit = {
    RiakConnector.invalidateSessionCache()
    mockSessionCache(2, Stream.continually(mock(classOf[RiakClient])).iterator)
  }

  @Test
  def openSessionWithDefaultParametersShouldReturnSessionFromPool(): Unit = {
    val session_1 = connector.openSession()
    val client_1 = session_1.unwrap()

    session_1.close()

    val session_2 = connector.openSession()
    val client_2 = session_2.unwrap()

    assertTrue(client_1 eq client_2)
  }

  @Test
  def openSessionWithHostsShouldReturnSessionFromPool(): Unit = {
    val hosts = HostAndPort.hostsFromString("2.2.2.2", 8780).toSeq

    val session_1 = connector.openSession(Some(hosts))
    val client_1 = session_1.unwrap()

    session_1.close()

    val session_2 = connector.openSession(Some(hosts))
    val client_2 = session_2.unwrap()

    assertTrue(client_1 eq client_2)
  }

  @Test
  def timeoutedSessionShouldBeEvicted(): Unit = {
    val hosts = HostAndPort.hostsFromString("2.2.2.2", 8780).toSeq

    val session1 = connector.openSession(Some(hosts))
    val client1 = session1.unwrap
    session1.close()
    
    Thread.sleep(RiakConnectorConf.defaultInactivityTimeout + 1000)

    val session2 = connector.openSession(Some(hosts))
    assertNotSame(client1, session2.unwrap)
  }

  @Test
  def sessionShouldNotBeEvictedWhenHasAliveRef(): Unit = {
    val hosts = HostAndPort.hostsFromString("2.2.2.2", 8780).toSeq

    val session1 = connector.openSession(Some(hosts))
    val session2 = connector.openSession(Some(hosts))

    assertSame(session1.unwrap, session2.unwrap) // session is in cache
   
    session1.close

    Thread.sleep(RiakConnectorConf.defaultInactivityTimeout + 1000)
    
    verify(session2.unwrap, never()).shutdown
    val session3 = connector.openSession(Some(hosts))
    assertSame(session2.unwrap, session3.unwrap) // session is still in cache
  }
  
  @Test
  def openSessionWithEqualHostsShouldReturnSessionFromPool(): Unit = {
    val hosts = conf.hosts.toSeq
    val fSession = connector.openSession()
    val sSession = connector.openSession(Some(hosts))

    assertNotSame(conf.hosts, hosts)
    assertEquals(conf.hosts, hosts.toSet)
    assertSame(fSession.unwrap(), sSession.unwrap())
  }

  @Test
  def openSessionWithNotEqualHostsShouldReturnDifferentSessions(): Unit = {
    val firstSession = connector.openSession()
    val secondSession = connector.openSession()
    val thirdSession = connector.openSession(Some(HostAndPort.hostsFromString("2.2.2.2", 0).toSeq))

    assertSame(firstSession.unwrap(), secondSession.unwrap())
    assertNotSame(firstSession.unwrap(), thirdSession.unwrap())
  }

  @Test
  def openSessionWithNewConfigShouldEvictCached(): Unit = {
    val firstSession = connector.openSession()
    val secondSession = connector.openSession(Some(HostAndPort.hostsFromString("2.2.2.2", 0).toSeq))

    // first session should be evicted from cache after this step
    val thirdSession = connector.openSession(Some(HostAndPort.hostsFromString("3.3.3.3", 0).toSeq))

    // check that all three sessions contains different client instances
    assertNotSame(firstSession.unwrap(), secondSession.unwrap())
    assertNotSame(firstSession.unwrap(), thirdSession.unwrap())

    // this session should be different from first due to eviction despite the same config
    val fourthSession = connector.openSession()
    assertNotSame(firstSession.unwrap(), fourthSession.unwrap())
  }

  @Test
  def evictedSessionWithNoReferencesShouldBeClosedImmediately(): Unit = {
    // create session
    val session = connector.openSession()
    val riakClient = session.unwrap()
    session.close()
    assertTrue(session.isClosed)

    // check that first session is still alive
    verify(riakClient, never()).shutdown()

    // check that first session is still in cache (the same RiakClient instance will be returned)
    val sameClientSession = connector.openSession()
    assertSame(riakClient, sameClientSession.unwrap())
    sameClientSession.close()

    // evict session (second opening is required due to cache size == 2)
    val newRiakClient = connector.openSession(Some(HostAndPort.hostsFromString("tmp1", 0).toSeq)).unwrap()
    connector.openSession(Some(HostAndPort.hostsFromString("tmp2", 0).toSeq))

    // check that first session was evicted and closed
    assertNotSame(riakClient, connector.openSession().unwrap())
    verify(riakClient, only()).shutdown()

    // check that second session was not closed (method "java.lang.AutoCloseable.close" was not called)
    verify(newRiakClient, never()).shutdown()
  }

  @Test
  def evictedSessionWithLiveReferenceShouldNotBeClosedImmediately(): Unit = {
    // create session
    val session = connector.openSession()
    val riakClient = session.unwrap()

    // check that first session is still in cache (the same RiakClient instance will be returned)
    val sameClientSession = connector.openSession()
    assertSame(riakClient, sameClientSession.unwrap())
    assertSame(session.unwrap(), sameClientSession.unwrap())
    sameClientSession.close()

    // evict session (second opening is required due to cache size == 2)
    connector.openSession(Some(HostAndPort.hostsFromString("tmp1", 0).toSeq))
    connector.openSession(Some(HostAndPort.hostsFromString("tmp2", 0).toSeq))

    // check that first session was evicted and closed
    assertNotSame(riakClient, connector.openSession().unwrap())
    verify(riakClient, never()).shutdown()
    assertFalse(session.isClosed)

    session.close()
    // check that session was closed
    verify(riakClient, only()).shutdown()
  }

  private def mockSessionCache(size: Int, func: => RiakClient): SessionCache = {
    // this names were received via reflection in runtime because usual name will cause NoSuchMethodException
    val createClientMethodName = "com$basho$riak$spark$rdd$connector$SessionCache$$createClient"
    val sessionCacheFieldName = "com$basho$riak$spark$rdd$connector$RiakConnector$$sessionCache"

    val sessionCacheSpy = spy(new SessionCache(size))

    FieldUtils.writeField(RiakConnector, sessionCacheFieldName, sessionCacheSpy, true)
    replace(method(classOf[SessionCache], createClientMethodName, classOf[RiakConnectorConf]))
      .`with`(new InvocationHandler {
        override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = func
      })
    sessionCacheSpy
  }

  private def mockSessionCache(size: Int, it: Iterator[RiakClient]): SessionCache = mockSessionCache(size, it.next())
}
