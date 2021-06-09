package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.{EqualTo, And}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AndFilterTest {
  private val schema = StructType(Array(
    StructField("integer", IntegerType),
    StructField("string", StringType),
    StructField("byte", ByteType)
  ))

  // FilterSupport.FULL
  private val f1 = EqualTo("string", "str")
  private val pushF1 = new EqualToFilter(f1, schema: StructType)

  // FilterSupport.FULL
  private val f2 = EqualTo("integer", 22)
  private val pushF2 = new EqualToFilter(f2, schema: StructType)

  // FilterSupport.NONE
  private val f3 = EqualTo("byte", 1.toByte)

  // FilterSupport.PARTIAL
  private val f4 = And(f1, f3)

  @Test
  def andFilterSupportFullFull(): Unit = {
    val andFilter = new AndFilter(And(f1, f2), schema)
    assertThat(andFilter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(andFilter.aql("d")).isEqualTo(s"""(${pushF1.aql("d")} AND ${pushF2.aql("d")})""")
  }

  @Test
  def andFilterSupportFullNone(): Unit = {
    val andFilter = new AndFilter(And(f1, f3), schema)
    assertThat(andFilter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(andFilter.aql("d")).isEqualTo(s"""(${pushF1.aql("d")})""")
  }

  @Test
  def andFilterSupportFullPartial(): Unit = {
    val andFilter = new AndFilter(And(f1, f4), schema)
    assertThat(andFilter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(andFilter.aql("d")).isEqualTo(s"""(${pushF1.aql("d")} AND (${pushF1.aql("d")}))""")
  }

  @Test
  def andFilterSupportPartialPartial(): Unit = {
    val andFilter = new AndFilter(And(f4, f4), schema)
    assertThat(andFilter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(andFilter.aql("d")).isEqualTo(s"""((${pushF1.aql("d")}) AND (${pushF1.aql("d")}))""")
  }

  @Test
  def andFilterSupportPartialNone(): Unit = {
    val andFilter = new AndFilter(And(f4, f3), schema)
    assertThat(andFilter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(andFilter.aql("d")).isEqualTo(s"""((${pushF1.aql("d")}))""")
  }

  @Test
  def andFilterSupportNoneNone(): Unit = {
    val andFilter = new AndFilter(And(f3, f3), schema)
    assertThat(andFilter.support()).isEqualTo(FilterSupport.NONE)
  }

}
