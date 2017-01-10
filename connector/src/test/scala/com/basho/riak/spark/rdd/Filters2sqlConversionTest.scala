package com.basho.riak.spark.rdd

import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.partitioner.RiakTSCoveragePlanBasedPartitioner
import org.apache.spark.sql.sources._
import org.junit.{Rule, Test}
import org.junit.runner.RunWith
import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner

@RunWith(classOf[MockitoJUnitRunner])
class Filters2sqlConversionTest {

  @Mock
  private val rc: RiakConnector = null

  val _expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def expectedException: ExpectedException = _expectedException

  private val bucketName = "test"

  private val equalTo = EqualTo("field1", "value1")
  private val lessThan = LessThan("field2", 2)
  private val lessThanOrEqual = LessThanOrEqual("field3", 3)
  private val greaterThan = GreaterThan("field4", 4)
  private val greaterThanOrEqual = GreaterThanOrEqual("field5", 5)
  private val isNotNull = IsNotNull("field6")
  private val isNull = IsNull("field7")

  // Unsupported filters
  private val equalNullSafe = EqualNullSafe("field", "value")
  private val in = In("field", Array("val0","val1"))
  private val and = And(equalTo, lessThan)
  private val or = Or(equalTo,lessThan)
  private val not = Not(equalTo)
  private val stringStartsWith = StringStartsWith("field","value")
  private val stringEndsWith = StringEndsWith("field","value")
  private val stringContains = StringContains("field","value")

  private def verifyFilters(expectedSql: String, filters: Filter*) = {
    val partitioner = new RiakTSCoveragePlanBasedPartitioner(rc, bucketName, None, None, filters.toArray, new ReadConf())
    assertEquals(expectedSql, partitioner.query)
  }

  private def verifyUnsupportedFilter(filter: Filter, expectedFilter: String) = {
    expectedException.expect(classOf[UnsupportedOperationException])
    expectedException.expectMessage(s"It's not a valid filter $expectedFilter " +
      s"to be pushed down, only is not null, is null, >, <, >=, <= and =  are allowed")
    new RiakTSCoveragePlanBasedPartitioner(rc, bucketName, None, None, Array(filter), new ReadConf())
  }

  @Test
  def testEqualToConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field1 = 'value1'", equalTo)
  }

  @Test
  def testLessThanConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field2 < 2", lessThan)
  }

  @Test
  def testLessThanOrEqualConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field3 <= 3", lessThanOrEqual)
  }

  @Test
  def testGreaterThanConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field4 > 4", greaterThan)
  }

  @Test
  def testGreaterThanOrEqualConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field5 >= 5", greaterThanOrEqual)
  }

  @Test
  def testIsNotNullConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field6 IS NOT NULL", isNotNull)
  }

  @Test
  def testIsNullConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field7 IS NULL", isNull)
  }

  @Test
  def testMultipleFiltersConversion(): Unit = {
    verifyFilters("SELECT * FROM test  WHERE field1 = 'value1' AND field2 < 2 AND field6 IS NOT NULL",
      equalTo,lessThan, isNotNull)
  }

  @Test
  def testUnsuportedFiltersEqualNullSafeConversion(): Unit = {
    verifyUnsupportedFilter(equalNullSafe, "EqualNullSafe(field,value)")
  }

  @Test
  def testUnsuportedFiltersInConversion(): Unit = {
    verifyUnsupportedFilter(in, "In(field, [val0,val1]")
  }

  @Test
  def testUnsuportedFiltersAndConversion(): Unit = {
    verifyUnsupportedFilter(and, "And(EqualTo(field1,value1),LessThan(field2,2))")
  }

  @Test
  def testUnsuportedFiltersOrConversion(): Unit = {
    verifyUnsupportedFilter(or, "Or(EqualTo(field1,value1),LessThan(field2,2))")
  }

  @Test
  def testUnsuportedFiltersNotConversion(): Unit = {
    verifyUnsupportedFilter(not, "Not(EqualTo(field1,value1))")
  }

  @Test
  def testUnsuportedFiltersStringStartsWithConversion(): Unit = {
    verifyUnsupportedFilter(stringStartsWith, "StringStartsWith(field,value)")
  }

  @Test
  def testUnsuportedFiltersStringEndsWithConversion(): Unit = {
    verifyUnsupportedFilter(stringEndsWith, "StringEndsWith(field,value)")
  }

  @Test
  def testUnsuportedFiltersStringContainsConversion(): Unit = {
    verifyUnsupportedFilter(stringContains, "StringContains(field,value)")
  }
}