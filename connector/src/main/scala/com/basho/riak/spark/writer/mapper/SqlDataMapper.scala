package com.basho.riak.spark.writer.mapper

import java.util.{Calendar, Date}

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{Cell, ColumnDescription, Row => RiakRow}
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.writer.ts.RowDef
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row => SparkRow}

/**
  * [[WriteDataMapper]] that converts Spark [[SparkRow]] objects to [[RiakRow]]
  */
class SqlDataMapper[T <: SparkRow] extends WriteDataMapper[T, RowDef] {

  override def mapValue(row: T): RowDef = {
    Option(row.schema) match {
      case None => RowDef(createRowByType(row), None)
      case Some(schema) =>
        val (riakRow, columnsDef) = createRowBySchema(row, schema)
        RowDef(riakRow, columnsDef)
    }
  }

  private def createRowBySchema(row: T, schema: StructType): (RiakRow, Option[Seq[ColumnDescription]]) = {
    val cellsWithDescription = schema.fields.map(field => getCellAndDescription(row, field))
    val (descriptions, cells) = cellsWithDescription.unzip
    new RiakRow(cells: _*) -> Option(descriptions)
  }

  private def getCellAndDescription(row: T, field: StructField): (ColumnDescription, Cell) = field.dataType match {
    case BooleanType =>
      val value = Option(row.getAs[Boolean](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.BOOLEAN)
      val cell = if (value.isDefined) new Cell(value.get) else null
      cd -> cell
    case StringType =>
      val value = Option(row.getAs[String](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.VARCHAR)
      val cell = if (value.isDefined) new Cell(value.get) else null
      cd -> cell
    case LongType =>
      val value = Option(row.getAs[Long](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = if (value.isDefined) new Cell(value.get) else null
      cd -> cell
    case IntegerType =>
      val value = Option(row.getAs[Int](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = if (value.isDefined) new Cell(value.get.toLong) else null
      cd -> cell
    case ShortType =>
      val value = Option(row.getAs[Short](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = if (value.isDefined) new Cell(value.get.toLong) else null
      cd -> cell
    case ByteType =>
      val value = Option(row.getAs[Byte](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = if (value.isDefined) new Cell(value.get.toLong) else null
      cd -> cell
    case FloatType =>
      val value = Option(row.getAs[Float](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.DOUBLE)
      val cell = if (value.isDefined) new Cell(value.get.toDouble) else null
      cd -> cell
    case DoubleType =>
      val value = Option(row.getAs[Double](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.DOUBLE)
      val cell = if (value.isDefined) new Cell(value.get) else null
      cd -> cell
    case DateType =>
      val sqlDate = Option(row.getAs[java.sql.Date](field.name))
      val cd = new ColumnDescription(field.name, ColumnType.TIMESTAMP)
      val cell = if (sqlDate.isDefined) new Cell(new Date(sqlDate.get.getTime)) else null
      cd -> cell
    case _ => sys.error(s"${field.dataType} not supported")
  }

  private def createRowByType(row: T): RiakRow = {
    val cells = for {i <- 0 until row.size} yield {
      row(i) match {
        case null => null
        case v: Boolean => new Cell(v)
        case v: String => new Cell(v)
        case v: Float => new Cell(v.toDouble)
        case v: Double => new Cell(v)
        case v: Long => new Cell(v)
        case v: Int => new Cell(v.toLong)
        case v: Short => new Cell(v.toLong)
        case v: Byte => new Cell(v.toLong)
        case v: Date => new Cell(v)
        case v: Calendar => new Cell(v)
        case _ => sys.error(s"Value of class ${row(i).getClass} not supported")
      }
    }
    new RiakRow(cells: _*)
  }
}

object SqlDataMapper {
  def factory[T <: SparkRow]: WriteDataMapperFactory[T, RowDef] =
    new WriteDataMapperFactory[T, RowDef] {
      override def dataMapper(bucket: BucketDef): WriteDataMapper[T, RowDef] =
        new SqlDataMapper[T]
    }
}