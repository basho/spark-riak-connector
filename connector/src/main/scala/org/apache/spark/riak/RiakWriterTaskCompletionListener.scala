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