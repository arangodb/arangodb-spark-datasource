package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.StringContains
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class StringContainsFilterTest {
  private val schema = StructType(Array(
    // atomic types
    StructField("bool", BooleanType),
    StructField("double", DoubleType),
    StructField("float", FloatType),
    StructField("integer", IntegerType),
    StructField("date", DateType),
    StructField("timestamp", TimestampType),
    StructField("short", ShortType),
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
  def stringContainsStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = PushableFilter(StringContains(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""CONTAINS(`d`.`$field`, "$value")""")
  }

  @Test
  def stringContainsFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = PushableFilter(StringContains(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.NONE)
  }

  @Test
  def stringContainsFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = PushableFilter(StringContains(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.NONE)
  }

}
