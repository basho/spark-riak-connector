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
package com.basho.riak.spark.writer

import com.basho.riak.client.core.query.timeseries.Row
import com.basho.riak.spark.writer.ts.RowDef

final class BulkIterator[T](data: Iterator[T],
                            batchSize: Int,
                            dataMapper: WriteDataMapper[T, RowDef]) extends Iterator[List[RowDef]] {

  override def hasNext: Boolean =
    data.hasNext

  override def next(): List[RowDef] = {
    data.take(batchSize).map(dataMapper.mapValue).toList
  }
}