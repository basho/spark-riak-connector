package com.basho.spark.connector.writer

import scala.reflect.runtime.universe._

import com.basho.spark.connector.rdd.BucketDef

trait ValueWriterFactory[T] {

  def valueWriter(bucket: BucketDef): ValueWriter[T]
}

trait LowPriorityValueWriterFactoryImplicits {
  implicit def defaultValueWriterFactory[T]: ValueWriterFactory[T] = DefaultValueWriter.factory

  implicit def tuple2ValueWriterFactory[A1: TypeTag, A2: TypeTag] = new ValueWriterFactory[(A1, A2)] {
    override def valueWriter(bucket: BucketDef): ValueWriter[(A1, A2)] = new TupleValueWriter
  }

  implicit def tuple3ValueWriterFactory[A1: TypeTag, A2: TypeTag, A3: TypeTag] = new ValueWriterFactory[(A1, A2, A3)] {
    override def valueWriter(bucket: BucketDef): ValueWriter[(A1, A2, A3)] = new TupleValueWriter
  }
}

object ValueWriterFactory extends LowPriorityValueWriterFactoryImplicits {
  implicit val genericRowWriterFactory = GenericValueWriter.Factory
}