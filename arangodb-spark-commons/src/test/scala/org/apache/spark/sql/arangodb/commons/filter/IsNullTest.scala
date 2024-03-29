package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.sources.{IsNull, IsNotNull}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class IsNullTest {
  private val schema = StructType(Array(
    StructField("a", StructType(Array(
      StructField("b", StringType)
    )))
  ))

  @Test
  def isNull(): Unit = {
    val isNullFilter = PushableFilter(IsNull("a.b"), schema)
    assertThat(isNullFilter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(isNullFilter.aql("d")).isEqualTo("""`d`.`a`.`b` == null""")
  }

  @Test
  def isNotNull(): Unit = {
    val isNotNullFilter = PushableFilter(IsNotNull("a.b"), schema)
    assertThat(isNotNullFilter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(isNotNullFilter.aql("d")).isEqualTo("""`d`.`a`.`b` != null""")
  }

}
