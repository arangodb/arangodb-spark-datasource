package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PushableFilterTest {
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

    StructField("n.a.m.e.", StructType(Array(
      StructField("first", StringType),
      StructField("last", StringType)
    )))
  ))

  @Test
  def equalToStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` == "$value"""")
  }

  @Test
  def equalToFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_COMPARE(`d`.`$field`, "$value", "years", "milliseconds")""")
  }

  @Test
  def equalToFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_COMPARE(`d`.`$field`, "$value", "years", "days")""")
  }

  @Test
  def equalToBoolFilter(): Unit = {
    val field = "bool"
    val value = false
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` == $value""")
  }

  @Test
  def equalToDoubleFilter(): Unit = {
    val field = "double"
    val value = 77.88
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` == $value""")
  }

  @Test
  def equalToFloatFilter(): Unit = {
    val field = "float"
    val value = 77.88F
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` == $value""")
  }

  @Test
  def equalToIntegerFilter(): Unit = {
    val field = "integer"
    val value = 22
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` == $value""")
  }

  @Test
  def equalToShortFilter(): Unit = {
    val field = "short"
    val value: Short = 22
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` == $value""")
  }

  @Test
  def equalToArrayFilter(): Unit = {
    val field = "array"
    val value = Seq("a","b","c")
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`array` == ["a","b","c"]""")
  }

}
