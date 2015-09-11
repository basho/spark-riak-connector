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
package com.basho.riak.spark.rdd

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