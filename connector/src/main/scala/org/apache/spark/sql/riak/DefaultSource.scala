/**
  * Copyright (c) 2015 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package org.apache.spark.sql.riak

import com.basho.riak.spark.rdd.ReadConf
import com.basho.riak.spark.rdd.connector.{RiakConnector, RiakConnectorConf}
import com.basho.riak.spark.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Riak data source extends [[RelationProvider]], [[SchemaRelationProvider]] and [[CreatableRelationProvider]].
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  
  /**
    * Creates a new relation for a RiakTS bucket and explicitly pass schema [[StructType]] as a parameter
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    DefaultSource.createRelationRead(sqlContext, parameters, Some(schema))
  }

  /**
    * Creates a new relation for a RiakTS bucket.
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    DefaultSource.createRelationRead(sqlContext, parameters, None)
  }

  /**
    * Creates a new relation for a RiakTS bucket and explicitly pass schema [[StructType]] as a parameter.
    * It saves the data to the RiakTS bucket depends on [[SaveMode]].
    *
    * @note Due to the RiakTS restriction the only [[SaveMode.Append]] is supported
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation =  DefaultSource.createRelationWrite(sqlContext, parameters, Some(data.schema))

    mode match {
      case SaveMode.Append => relation.insert(data, overwrite = false)
      case _ =>
          throw new UnsupportedOperationException(s"Writing with mode '$mode' is not supported, 'Append' is the only supported mode")
    }
    relation
  }
}

//TODO: consider whether BucketDef is really needed
private case class BucketDef(bucket: String, schema: Option[StructType])

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
object DefaultSource {
  val RiakBucketProperty = "path"

  // In case of using SQLContext.load(String)
  val RiakDataSourceProviderPackageName = DefaultSource.getClass.getPackage.getName
  val RiakDataSourceProviderClassName = RiakDataSourceProviderPackageName + ".DefaultSource"

  private def parseBucketDef(parameters: Map[String, String]): BucketDef = {
    val bucket = parameters(RiakBucketProperty)
    BucketDef(bucket, None)
  }
  
  private def parseRiakConnectionOptions(options: Map[String, String], conf: SparkConf): RiakConnector = {
    new RiakConnector(RiakConnectorConf(conf, options))
  }
  
  private def createRelationRead(sqlContext: SQLContext, parameters: Map[String, String], schema: Option[StructType]): RiakRelation = {
    val existingConf = sqlContext.sparkContext.getConf
    val bucketDef = parseBucketDef(parameters)
    val riakConnector = parseRiakConnectionOptions(parameters, existingConf)
    val readConf = ReadConf(existingConf, parameters)
    RiakRelation(bucketDef.bucket, sqlContext, schema, Some(riakConnector), readConf = readConf)
  }
  
  private def createRelationWrite(sqlContext: SQLContext, parameters: Map[String, String], schema: Option[StructType]): RiakRelation = {
    val existingConf = sqlContext.sparkContext.getConf
    val bucketDef = parseBucketDef(parameters)
    val riakConnector = parseRiakConnectionOptions(parameters, existingConf)
    val writeConf = WriteConf(existingConf, parameters)
    RiakRelation(bucketDef.bucket, sqlContext, schema, Some(riakConnector), writeConf = writeConf)
  }
}

