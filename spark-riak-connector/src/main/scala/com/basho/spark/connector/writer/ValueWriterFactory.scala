package com.basho.spark.connector.writer

import com.basho.spark.connector.rdd.BucketDef

trait ValueWriterFactory[T] {

  def valueWriter(bucket: BucketDef): ValueWriter[T]
}

trait LowPriorityValueWriterFactoryImplicits {
  implicit def defaultValueWriterFactory[T]: ValueWriterFactory[T] = DefaultValueWriter.factory
}

object ValueWriterFactory extends LowPriorityValueWriterFactoryImplicits {
  implicit val genericRowWriterFactory = GenericValueWriter.Factory
}