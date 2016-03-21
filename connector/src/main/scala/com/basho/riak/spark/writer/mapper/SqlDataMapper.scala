package com.basho.riak.spark.writer.mapper

import java.util.{Calendar, Date}
import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{Cell, ColumnDescription, Row => RiakRow}
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.writer.ts.RowDef
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row => SparkRow}
import com.basho.riak.spark.util.TSConversionUtil._

/**
  * [[WriteDataMapper]] that converts Spark [[SparkRow]] objects to [[RiakRow]]
  */
class SqlDataMapper[T <: SparkRow] extends WriteDataMapper[T, RowDef] {

  override def mapValue(row: T): RowDef = {
    Option(row.schema) match {
      case None => RowDef(createRowByType(row), None)
      case Some(schema) =>
        val (riakRow, columnsDef) = createRiakRowBySchema(row, schema)
        RowDef(riakRow, columnsDef)
    }
  }
}

object SqlDataMapper {
  def factory[T <: SparkRow]: WriteDataMapperFactory[T, RowDef] =
    new WriteDataMapperFactory[T, RowDef] {
      override def dataMapper(bucket: BucketDef): WriteDataMapper[T, RowDef] =
        new SqlDataMapper[T]
    }
}