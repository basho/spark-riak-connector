package com.basho.riak.spark.query

import com.basho.riak.client.core.query.timeseries.Row
import com.basho.riak.client.core.query.{ Location, RiakObject }
import org.apache.spark.Logging
import com.basho.riak.spark.util.DataConvertingIterator
import com.basho.riak.spark.util.TSConversionUtil
import org.apache.spark.sql.types.StructType
import com.basho.riak.spark.rdd.TsTimestampBindingType
import com.basho.riak.client.core.query.timeseries.ColumnDescription
import scala.reflect.ClassTag

class TSDataQueryingIterator(query: QueryTS) extends Iterator[Row] with Logging {

  private var _iterator: Option[Iterator[Row]] = None
  private val subqueries = query.queryData.iterator
  private var columns: Option[Seq[ColumnDescription]] = None

  def columnDefs: Seq[ColumnDescription] = {
    if (!columns.isDefined) {
      // if columns were requested go and load data
      prefetch()
    }
    columns match {
      case None      => Seq()
      case Some(cds) => cds
    }
  }
  
  protected[this] def prefetch() = {
    val r = query.nextChunk(subqueries.next)
    r match {
      case (cds, rows) =>
        logDebug(s"Returned ${rows.size} rows")
        if(!columns.isDefined)
          columns = Some(cds)
        _iterator = Some(rows.iterator)
      case _ => _iterator = None
    }
  }

  override def hasNext: Boolean = {
    if (subqueries.hasNext) {
      if (_iterator.isEmpty || (_iterator.isDefined && !_iterator.get.hasNext))
        prefetch()
    }
    _iterator match {
      case Some(it) => it.hasNext
      case None     => false
    }
  }

  override def next(): Row = {
    if (!hasNext) {
      throw new NoSuchElementException("next on empty iterator")
    }
    _iterator.get.next
  }
}

object TSDataQueryingIterator {

  def apply[R](query: QueryTS): TSDataQueryingIterator = new TSDataQueryingIterator(query)
}
