package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.{And, EqualTo, Not}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class NotFilterTest {
  private val schema = StructType(Array(
    StructField("integer", IntegerType),
    StructField("string", StringType),
    StructField("byte", ByteType)
  ))

  // FilterSupport.FULL
  private val f1 = EqualTo("string", "str")
  private val pushF1 = PushableFilter(f1, schema: StructType)

  // FilterSupport.NONE
  private val f2 = EqualTo("byte", 1.toByte)

  // FilterSupport.PARTIAL
  private val f3 = And(f1, f2)

  @Test
  def notFilterSupportFull(): Unit = {
    val notFilter = PushableFilter(Not(f1), schema)
    assertThat(notFilter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(notFilter.aql("d")).isEqualTo(s"""NOT (${pushF1.aql("d")})""")
  }

  @Test
  def notFilterSupportNone(): Unit = {
    val notFilter = PushableFilter(Not(f2), schema)
    assertThat(notFilter.support()).isEqualTo(FilterSupport.NONE)
  }

  @Test
  def notFilterSupportPartial(): Unit = {
    val notFilter = PushableFilter(Not(f3), schema)
    assertThat(notFilter.support()).isEqualTo(FilterSupport.NONE)
  }

}
