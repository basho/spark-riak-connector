package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.metrics.source.Source



/**
 * @author anekhaev
 */
class RiakConnectorSource extends Source {
  
  override val sourceName = "riak-connector"

  override val metricRegistry = new metrics.MetricRegistry
  
  val locationsFull = metricRegistry.timer("data-chunk.locations.full")

  val valuesFull = metricRegistry.timer("data-chunk.values.full")

  val locationsNotFull = metricRegistry.timer("data-chunk.locations.notFull")

  val valuesNotFull = metricRegistry.timer("data-chunk.values.notFull")

  val dataChunkFull = metricRegistry.timer("data-chunk.full")

  val dataChunkNotFull = metricRegistry.timer("data-chunk.notFull")

  val emptyChunk = metricRegistry.meter("data-chunk.empty")

  val erroredChunk = metricRegistry.meter("data-chunk.error")

  RiakConnectorSource.registerInstance(this)
  
}


object RiakConnectorSource {
  
  private var inst: Option[RiakConnectorSource] = None
  
  def registerInstance(source: RiakConnectorSource) = synchronized {
    inst = Some(source)
  }
  
  def instance = inst
  
}