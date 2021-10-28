package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LessThanOrEqualFilterTest {
  private val schema = StructType(Array(
    // atomic types
    StructField("bool", BooleanType),
    StructField("double", DoubleType),
    StructField("float", FloatType),
    StructField("integer", IntegerType),
    StructField("long", LongType),
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
  def lessThanOrEqualStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= "$value"""")
  }

  @Test
  def lessThanOrEqualFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_TIMESTAMP(`d`.`$field`) <= DATE_TIMESTAMP("$value")""")
  }

  @Test
  def lessThanOrEqualFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_TIMESTAMP(`d`.`$field`) <= DATE_TIMESTAMP("$value")""")
  }

  @Test
  def lessThanOrEqualBoolFilter(): Unit = {
    val field = "bool"
    val value = false
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualDoubleFilter(): Unit = {
    val field = "double"
    val value = 77.88
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualFloatFilter(): Unit = {
    val field = "float"
    val value = 77.88F
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualIntegerFilter(): Unit = {
    val field = "integer"
    val value = 22
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualLongFilter(): Unit = {
    val field = "long"
    val value = 22L
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualShortFilter(): Unit = {
    val field = "short"
    val value: Short = 22
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualByteFilter(): Unit = {
    val field = "byte"
    val value: Byte = 22
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` <= $value""")
  }

  @Test
  def lessThanOrEqualArrayFilter(): Unit = {
    val field = "array"
    val value = Seq("a", "b", "c")
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`array` <= ["a","b","c"]""")
  }

  @Test
  def lessThanOrEqualStructFilter(): Unit = {
    val field = "struct"
    val value = new GenericRowWithSchema(
      Array("str", 22),
      StructType(Array(
        StructField("a", StringType),
        StructField("b", IntegerType)
      ))
    )
    val filter = PushableFilter(LessThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`struct` <= {"a":"str","b":22}""")
  }

}
