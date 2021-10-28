package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.StringStartsWith
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class StringStartsWithFilterTest {
  private val schema = StructType(Array(
    // atomic types
    StructField("bool", BooleanType),
    StructField("double", DoubleType),
    StructField("float", FloatType),
    StructField("integer", IntegerType),
    StructField("date", DateType),
    StructField("timestamp", TimestampType),
    StructField("short", ShortType),
    StructField("byte", ByteType),
    StructField("string", StringType),

    // complex types
    StructField("array", ArrayType(StringType)),
    StructField("null", NullType),
    StructField("struct", StructType(Array(
      StructField("a", StringType),
      StructField("b", IntegerType)
    )))
  ))

  @Test
  def stringStartsWithStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = PushableFilter(StringStartsWith(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""STARTS_WITH(`d`.`$field`, "$value")""")
  }

  @Test
  def stringStartsWithFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = PushableFilter(StringStartsWith(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.NONE)
  }

  @Test
  def stringStartsWithFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = PushableFilter(StringStartsWith(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.NONE)
  }

}
