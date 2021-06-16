package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.StringEndsWith
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class StringEndsWithFilterTest {
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
  def stringEndsWithStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = PushableFilter(StringEndsWith(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""STARTS_WITH(REVERSE(`d`.`$field`), REVERSE("$value"))""")
  }

  @Test
  def stringEndsWithFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = PushableFilter(StringEndsWith(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.NONE)
  }

  @Test
  def stringEndsWithFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = PushableFilter(StringEndsWith(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.NONE)
  }

}
