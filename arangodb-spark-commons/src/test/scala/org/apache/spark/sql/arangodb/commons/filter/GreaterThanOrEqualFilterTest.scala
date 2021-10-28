package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class GreaterThanOrEqualFilterTest {
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
  def greaterThanOrEqualStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= "$value"""")
  }

  @Test
  def greaterThanOrEqualFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_TIMESTAMP(`d`.`$field`) >= DATE_TIMESTAMP("$value")""")
  }

  @Test
  def greaterThanOrEqualFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_TIMESTAMP(`d`.`$field`) >= DATE_TIMESTAMP("$value")""")
  }

  @Test
  def greaterThanOrEqualBoolFilter(): Unit = {
    val field = "bool"
    val value = false
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= $value""")
  }

  @Test
  def greaterThanOrEqualDoubleFilter(): Unit = {
    val field = "double"
    val value = 77.88
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= $value""")
  }

  @Test
  def greaterThanOrEqualFloatFilter(): Unit = {
    val field = "float"
    val value = 77.88F
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= $value""")
  }

  @Test
  def greaterThanOrEqualIntegerFilter(): Unit = {
    val field = "long"
    val value = 22L
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= $value""")
  }

  @Test
  def greaterThanOrEqualShortFilter(): Unit = {
    val field = "short"
    val value: Short = 22
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= $value""")
  }

  @Test
  def greaterThanOrEqualByteFilter(): Unit = {
    val field = "byte"
    val value: Byte = 22
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` >= $value""")
  }

  @Test
  def greaterThanOrEqualArrayFilter(): Unit = {
    val field = "array"
    val value = Seq("a", "b", "c")
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`array` >= ["a","b","c"]""")
  }

  @Test
  def greaterThanOrEqualStructFilter(): Unit = {
    val field = "struct"
    val value = new GenericRowWithSchema(
      Array("str", 22),
      StructType(Array(
        StructField("a", StringType),
        StructField("b", IntegerType)
      ))
    )
    val filter = PushableFilter(GreaterThanOrEqual(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`struct` >= {"a":"str","b":22}""")
  }

}
