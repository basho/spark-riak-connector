package com.basho.spark.connector.rdd

import java.util.concurrent.atomic.LongAdder

import org.slf4j.Logger

import scala.concurrent.duration.Duration

class StatCounter(logger:Logger = null) {
  case class Stats(duration: Duration, counter:Long, logger:Logger = null){
    def dump(message: String, logger:Logger = this.logger ):Stats ={
      require(logger != null, "logger should be specified")
      logger.info("{}\n\t{} items were processed\n\tit took {}\n",
        List[AnyRef](message, counter: java.lang.Long, duration):_*)
      this
    }
  }

  private val counter = new LongAdder
  private var startedAt = System.currentTimeMillis()


  def increment():StatCounter = {
    counter.increment()
    this
  }

  def +=(value: Int): StatCounter = {
    counter.add(value)
    this
  }

  def +=(value: Long): StatCounter = {
    counter.add(value)
    this
  }

  def reset():StatCounter = {
    startedAt = System.currentTimeMillis()
    counter.reset()
    this
  }

  def stats():Stats ={
    val duration = System.currentTimeMillis() - startedAt
    new Stats(Duration(duration, "ms"), counter.longValue(), logger)
  }
}

object StatCounter{
  def apply(logger: Logger = null): StatCounter = {
    new StatCounter(logger)
  }
}