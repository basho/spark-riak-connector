package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.metrics.source.Source
import com.codahale.metrics.Timer
import com.codahale.metrics.UniformReservoir



/**
 * @author anekhaev
 */
class RiakConnectorSource extends Source {
  
  override val sourceName = "riak-connector"

  override val metricRegistry = new metrics.MetricRegistry

  val fbr1Full = timer("fbr-one-query.full")

  val fbr1NotFull = timer("fbr-one-query.notFull")

  val fbr2LocationsFull = timer("fbr-two-queries.locations.full")

  val fbr2ValuesFull = timer("fbr-two-queries.values.full")

  val fbr2LocationsNotFull = timer("fbr-two-queries.locations.notFull")

  val fbr2ValuesNotFull = timer("fbr-two-queries.values.notFull")

  val fbr2Full = timer("fbr-two-queries.full")

  val fbr2NotFull = timer("fbr-two-queries.notFull")

  val fbr2EmptyChunk = metricRegistry.meter("fbr-two-queries.empty")

  val fbr2ErrorChunk = metricRegistry.meter("fbr-two-queries.error")

  RiakConnectorSource.registerInstance(this)

  
  def timer(name: String) = {
    metricRegistry.register(name, new Timer(new UniformReservoir()))
  }
}




object RiakConnectorSource {
  
  private var inst: Option[RiakConnectorSource] = None
  
  def registerInstance(source: RiakConnectorSource) = synchronized {
    inst = Some(source)
  }
  
  def instance = inst
  
}