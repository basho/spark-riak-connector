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
package org.apache.spark.sql.riak

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
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation =
    RiakRelation(sqlContext, parameters, Some(schema))

  /**
    * Creates a new relation for a RiakTS bucket.
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    RiakRelation(sqlContext, parameters, None)

  /**
    * Creates a new relation for a RiakTS bucket and explicitly pass schema [[StructType]] as a parameter.
    * It saves the data to the RiakTS bucket depends on [[SaveMode]].
    *
    * @note Due to the RiakTS restriction the only [[SaveMode.Append]] is supported
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation =  RiakRelation(sqlContext, parameters, Some(data.schema))

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
}

