package com.basho.riak.spark.writer.ts

import com.basho.riak.client.core.query.timeseries.{ColumnDescription, Row}

case class RowDef(row: Row, columnDescription: Option[Seq[ColumnDescription]])