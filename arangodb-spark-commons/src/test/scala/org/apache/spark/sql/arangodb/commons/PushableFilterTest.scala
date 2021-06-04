package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType, TimestampType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PushableFilterTest {
  private val schema = StructType(Array(
    StructField("birthday", DateType),
    StructField("gender", StringType),
    StructField("timestamp", TimestampType),
    StructField("likes", ArrayType(StringType)),
    StructField("n.a.m.e.", StructType(Array(
      StructField("first", StringType),
      StructField("last", StringType)
    )))
  ))

  @Test
  def equalToFilter(): Unit = {
    val field = "gender"
    val value = "female"
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
    val field = "birthday"
    val value = "2001-01-02"
    val filter = new EqualToFilter(EqualTo(field, value), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d")).isEqualTo(s"""DATE_COMPARE(`d`.`$field`, "$value", "years", "days")""")
  }

}
