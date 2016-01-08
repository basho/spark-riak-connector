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

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
  * Riak data source extends [[RelationProvider]], [[SchemaRelationProvider]] and [[CreatableRelationProvider]].
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val bucketDef = DefaultSource.parseBucketDef(parameters)
    RiakRelation(bucketDef.bucket, sqlContext, Some(schema))
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val bucketDef = DefaultSource.parseBucketDef(parameters)
    RiakRelation(bucketDef.bucket, sqlContext, None)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val bucketDef = DefaultSource.parseBucketDef(parameters)
    val relation =  RiakRelation(bucketDef.bucket, sqlContext, None)

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
}

