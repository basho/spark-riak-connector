package org.apache.spark.riak.types

import java.util.concurrent.TimeUnit

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{ColumnDescription, FullColumnDescription}
import com.basho.riak.spark.rdd.{TsTimestampBindingType, UseLong, UseTimestamp}
import com.basho.riak.spark.util.TSConversionUtil
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, TimestampType, _}
import org.apache.spark.riak.Logging

import scala.language.postfixOps

/**
  * Custom RiakTS StructType
  *
  * @param fields fields of schema
  * @param quantum TS table quantum milliseconds
  * @param quantizedField name of quantized column
  */
class RiakStructType(override val fields: Array[StructField],
                     val quantum: Option[Long] = None,
                     val quantizedField: Option[String] = None) extends StructType(fields)

object RiakStructType extends Logging {
  private val partitionKeyOrdinalProp = "riak.partitionKeyOrdinal"
  private val localKeyOrdinalProp = "riak.localKeyOrdinal"
  private val quantum = "riakTS.quantum"
  private val quantizedField = "riakTS.quantizedField"

  def apply(fields: Seq[StructField], quantum: Long, quantizedField: String): RiakStructType = {
    new RiakStructType(fields.toArray, Some(quantum), Some(quantizedField))
  }

  def apply(fields: Seq[StructField], quantizedField: String): RiakStructType = {
    new RiakStructType(fields.toArray, None, Some(quantizedField))
  }

  def apply(columns: Seq[ColumnDescription],
            tsTimestampBinding: TsTimestampBindingType,
            quantum: Option[Long] = None,
            quantizedField: Option[String] = None): RiakStructType = {
    val fields = columns.map(c => asStructField(c, tsTimestampBinding))
    new RiakStructType(fields.toArray, quantum, quantizedField)
  }

  private def getQuantumAndField(fields: Array[StructField]): Option[(Long,String)] = {
    fields filter { _.metadata.contains(TSConversionUtil.quantum)
    } filter { _.metadata.contains(TSConversionUtil.quantizedField)
    } toList match {
      case x :: Nil =>
        val quantum = x.metadata.getLong(TSConversionUtil.quantum)
        val quantizedField = x.metadata.getString(TSConversionUtil.quantizedField)
        Some(quantum,quantizedField)
      case x :: xs =>
        val message =
          s"""
             |Table definition has More than one quantum.
             |Columns contain quantum: ${(x :: xs) map { _.name} mkString ","}
            """.stripMargin
        throw new IllegalStateException(message)
      case Nil =>
        val message =
          s"""
             |Unable to retrieve quantum from Riak TS.
             |Probably you are using Riak TS with version 1.3 or bellow.
             |In this case quantum should be defined explicitly.
           """.stripMargin
        logInfo(message)
        None
    }
  }

  private def asStructField(columnDescription: ColumnDescription, tsTimestampBinding: TsTimestampBindingType): StructField = {
    val ft = asDataType(columnDescription.getType, tsTimestampBinding)
    columnDescription match {
      case fullColumnDescription: FullColumnDescription =>
        val partitionKeyOrdinal = fullColumnDescription.getPartitionKeyOrdinal
        val localKeyOrdinal = fullColumnDescription.getLocalKeyOrdinal
        val metadataBuilder = new MetadataBuilder()

        if (Option(localKeyOrdinal).nonEmpty) {
          metadataBuilder.putLong(localKeyOrdinalProp, localKeyOrdinal.toLong)
        }

        if (Option(partitionKeyOrdinal).nonEmpty) {
          metadataBuilder.putLong(partitionKeyOrdinalProp, partitionKeyOrdinal.toLong)
        }

        if (fullColumnDescription.getType == ColumnType.TIMESTAMP && fullColumnDescription.isPartitionKeyMember) {
          Option(fullColumnDescription.getQuantum) match {
            case Some(q) =>
              metadataBuilder.putLong(quantum, TimeUnit.MILLISECONDS.convert(q.getInterval, q.getUnit))
              metadataBuilder.putString(quantizedField, fullColumnDescription.getName)
            case None =>
          }
        }
        val metadata = metadataBuilder.build()
        StructField(columnDescription.getName, ft, fullColumnDescription.isNullable, metadata)
      case _ =>
        StructField(columnDescription.getName, ft)
    }
  }

  def asDataType(columnType: ColumnType, tsTimestampBinding: TsTimestampBindingType): DataType = {
    columnType match {
      case ColumnType.BOOLEAN => BooleanType
      case ColumnType.DOUBLE  => DoubleType
      case ColumnType.SINT64  => LongType
      case ColumnType.TIMESTAMP => tsTimestampBinding match {
        case UseLong      => LongType
        case UseTimestamp => TimestampType
      }
      case ColumnType.VARCHAR => StringType
      case _ => throw new IllegalStateException(s"Unsupported column type $columnType")
    }
  }
}