/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.rdd

import scala.reflect.runtime.universe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.junit.Assert._
import org.junit.{ Before, Test }

import com.basho.riak.spark.toSparkContextFunctions

case class TestData(id: String, name: String, age: Int, category: String)

class SparkDataframesTest extends AbstractRDDTest {

  private val indexName = "creationNo"

  protected override def jsonData(): String =
    "[" +
      "  {key: 'key1', value: {id: 'u1', name: 'Ben', age: 20, category: 'CategoryA'}}" +
      ", {key: 'key2', value: {id: 'u2', name: 'Clair', age: 30, category: 'CategoryB'}}" +
      ", {key: 'key3', value: {id: 'u3', name: 'John', age: 70}}" +
      ", {key: 'key4', value: {id: 'u4', name: 'Chris', age: 10, category: 'CategoryC'}}" +
      ", {key: 'key5', value: {id: 'u5', name: 'Mary', age: 40, category: 'CategoryB'}}" +
      ", {key: 'key6', value: {id: 'u6', name: 'George', age: 50, category: 'CategoryC'}}" +
      "]"
  protected override def initSparkConf = {
    super.initSparkConf().setAppName("Dataframes Test")
  }

  private def stringify = (s: Array[String]) => s.mkString("[", ",", "]")

  var sqlContextHolder: SQLContext = null
  var df: DataFrame = null

  @Before
  def initializeDF(): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContextHolder = sqlContext
    df = sc.riakBucket[TestData](DEFAULT_NAMESPACE.getBucketNameAsString)
      .queryAll.toDF
    df.registerTempTable("test")
  }

  @Test
  def schemaTest(): Unit = {
    df.printSchema()
    val schema = df.schema.map(_.name).toList
    val fields = universe.typeOf[TestData].members.withFilter(!_.isMethod).map(_.name.toString.trim).toList
    assertEquals(schema.sorted, fields.sorted)
  }

  @Test
  def sqlQueryTest(): Unit = {
    val sqlResult = sqlContextHolder.sql("select * from test where category >= 'CategoryC'").toJSON.collect
    val expected = "[" +
      "{id:'u4',name:'Chris',age:10,category:'CategoryC'}," + 
      "{id:'u6',name:'George',age:50,category:'CategoryC'}" +
      "]"
    assertEqualsUsingJSONIgnoreOrder(expected, stringify(sqlResult))
  }

  @Test
  def udfTest(): Unit = {
    sqlContextHolder.udf.register("stringLength", (s: String) => s.length)
    val udf = sqlContextHolder.sql("select name, stringLength(name) strLgth from test order by strLgth, name").toJSON.collect
    val expected = "[" +
      "{name:'Ben',strLgth:3}," +
      "{name:'John',strLgth:4}," + 
      "{name:'Mary',strLgth:4}," +
      "{name:'Chris',strLgth:5}," +
      "{name:'Clair',strLgth:5}," +
      "{name:'George',strLgth:6}" +
      "]"
    assertEqualsUsingJSON(expected, stringify(udf))
  }

  @Test
  def grouppingTest(): Unit = {
    val groupped = df.groupBy("category").count.toJSON.collect
    val expected = "[" +
      "{category:'CategoryA',count:1}," +
      "{category:'CategoryB',count:2}," +
      "{category:'CategoryC',count:2}," +
      "{count:1}" +
      "]"
    assertEqualsUsingJSONIgnoreOrder(expected, stringify(groupped))
  }

  @Test
  def sqlVsFilterTest(): Unit = {
    val sql = sqlContextHolder.sql("select id, name from test where age >= 50").toJSON.collect
    val filtered = df.where(df("age") >= 50).select("id", "name").toJSON.collect
    assertEqualsUsingJSONIgnoreOrder(stringify(sql), stringify(filtered))
  }

}