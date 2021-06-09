package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.{And, Or, EqualTo}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OrFilterTest {
  private val schema = StructType(Array(
    StructField("integer", IntegerType),
    StructField("string", StringType),
    StructField("byte", ByteType)
  ))

  // FilterSupport.FULL
  private val f1 = EqualTo("string", "str")
  private val pushF1 = new EqualToFilter(f1, schema: StructType)

  // FilterSupport.NONE
  private val f2 = EqualTo("byte", 1.toByte)

  // FilterSupport.PARTIAL
  private val f3 = And(f1, f2)

  @Test
  def orFilterSupportFullFull(): Unit = {
    val orFilter = new OrFilter(Or(f1, f1), schema)
    assertThat(orFilter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(orFilter.aql("d")).isEqualTo(s"""(${pushF1.aql("d")} OR ${pushF1.aql("d")})""")
  }

  @Test
  def orFilterSupportFullNone(): Unit = {
    val orFilter = new OrFilter(Or(f1, f2), schema)
    assertThat(orFilter.support()).isEqualTo(FilterSupport.NONE)
  }

  @Test
  def orFilterSupportFullPartial(): Unit = {
    val orFilter = new OrFilter(Or(f1, f3), schema)
    assertThat(orFilter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(orFilter.aql("d")).isEqualTo(s"""(${pushF1.aql("d")} OR (${pushF1.aql("d")}))""")
  }

  @Test
  def orFilterSupportPartialPartial(): Unit = {
    val orFilter = new OrFilter(Or(f3, f3), schema)
    assertThat(orFilter.support()).isEqualTo(FilterSupport.PARTIAL)
    assertThat(orFilter.aql("d")).isEqualTo(s"""((${pushF1.aql("d")}) OR (${pushF1.aql("d")}))""")
  }

  @Test
  def orFilterSupportPartialNone(): Unit = {
    val orFilter = new OrFilter(Or(f3, f2), schema)
    assertThat(orFilter.support()).isEqualTo(FilterSupport.NONE)
  }

  @Test
  def orFilterSupportNoneNone(): Unit = {
    val orFilter = new OrFilter(Or(f2, f2), schema)
    assertThat(orFilter.support()).isEqualTo(FilterSupport.NONE)
  }

}
