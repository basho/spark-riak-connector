package org.apache.spark.metrics


import org.apache.spark.metrics.source.Source
import com.codahale.metrics



/**
 * @author anekhaev
 */
class RiakConnectorSource extends Source {
  
  override val sourceName = "riak-connector"

  override val metricRegistry = new metrics.MetricRegistry
  
  val dataQueryingIteratorTimer = metricRegistry.timer("data-querying-iterator-timer")
  
  RiakConnectorSource.registerInstance(this)
  
}


object RiakConnectorSource {
  
  private var inst: Option[RiakConnectorSource] = None
  
  def registerInstance(source: RiakConnectorSource) = synchronized {
    inst = Some(source)
  }
  
  def instance = inst
  
}