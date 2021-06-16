package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LessThanFilterTest {
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
  def lessThanStringFilter(): Unit = {
    val field = "string"
    val value = "str"
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` < "$value"""")
  }

  @Test
  def lessThanFilterTimestamp(): Unit = {
    val field = "timestamp"
    val value = "2001-01-02T15:30:45.678111Z"
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_TIMESTAMP(`d`.`$field`) < $value""")
  }

  @Test
  def lessThanFilterDate(): Unit = {
    val field = "date"
    val value = "2001-01-02"
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_TIMESTAMP(`d`.`$field`) < "$value"""")
  }

  @Test
  def lessThanBoolFilter(): Unit = {
    val field = "bool"
    val value = false
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` < $value""")
  }

  @Test
  def lessThanDoubleFilter(): Unit = {
    val field = "double"
    val value = 77.88
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` < $value""")
  }

  @Test
  def lessThanFloatFilter(): Unit = {
    val field = "float"
    val value = 77.88F
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` < $value""")
  }

  @Test
  def lessThanIntegerFilter(): Unit = {
    val field = "integer"
    val value = 22
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` < $value""")
  }

  @Test
  def lessThanShortFilter(): Unit = {
    val field = "short"
    val value: Short = 22
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`$field` < $value""")
  }

  @Test
  def lessThanArrayFilter(): Unit = {
    val field = "array"
    val value = Seq("a", "b", "c")
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`array` < ["a","b","c"]""")
  }

  @Test
  def lessThanStructFilter(): Unit = {
    val field = "struct"
    val value = new GenericRowWithSchema(
      Array("str", 22),
      StructType(Array(
        StructField("a", StringType),
        StructField("b", IntegerType)
      ))
    )
    val filter = PushableFilter(LessThan(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""`d`.`struct` < {"a":"str","b":22}""")
  }

}
