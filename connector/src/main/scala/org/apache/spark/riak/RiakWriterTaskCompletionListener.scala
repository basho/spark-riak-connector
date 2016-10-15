/*******************************************************************************
  * Copyright (c) 2016 IBM Corp.
  *
  * Created by Basho Technologies for IBM
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *******************************************************************************/
package org.apache.spark.riak

import org.apache.spark.TaskContext
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.util.TaskCompletionListener

class RiakWriterTaskCompletionListener(recordsWritten: Long) extends TaskCompletionListener{

  override def onTaskCompletion(context: TaskContext): Unit = {
    val metrics = OutputMetrics(DataWriteMethod.Hadoop)
    metrics.setRecordsWritten(recordsWritten)
    context.taskMetrics().outputMetrics = Some(metrics)
  }

}

object RiakWriterTaskCompletionListener {
  def apply(recordsWritten: Long) = new RiakWriterTaskCompletionListener(recordsWritten)
}