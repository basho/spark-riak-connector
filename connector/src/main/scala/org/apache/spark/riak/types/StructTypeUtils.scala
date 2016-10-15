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
package org.apache.spark.riak.types

import com.basho.riak.client.core.query.timeseries.TableDefinition
import com.basho.riak.spark.util.TSConversionUtil
import org.apache.spark.Logging
import org.apache.spark.sql.types.StructType

object StructTypeUtils {
  implicit class RiakStructType(val struckType: StructType) extends Logging {
    def getQuantum: Option[Long] = {
      struckType.fields filter { _.metadata.contains(TSConversionUtil.quantum) } toList match {
        case x :: Nil => Some(x.metadata.getLong(TSConversionUtil.quantum))
        case x :: xs =>
          val message =
            s"""
              |Table definition has More than one quantum.
              |Collumns contain quantum: ${(x :: xs) map { _.name} mkString ","}
            """.stripMargin
          throw new IllegalStateException(message)
        case Nil => None
      }
    }
  }

  implicit def structTypeToRiakStructType(structType: StructType, tableDef: TableDefinition): RiakStructType =
      new RiakStructType(structType)
}
