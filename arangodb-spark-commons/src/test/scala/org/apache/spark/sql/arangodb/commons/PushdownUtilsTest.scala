package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PushdownUtilsTest {

  private val schema = StructType(Array(
    StructField(""""birthday"""", DateType),
    StructField("gender", StringType),
    StructField("likes", ArrayType(StringType)),
    StructField("n.a.m.e.", StructType(Array(
      StructField("first", StringType),
      StructField("last", StringType)
    )))
  ))

  @Test
  def equalToFilter(): Unit = {
    val f = new EqualToFilter(EqualTo("`n.a.m.e.`.first", "fname"), schema: StructType)
    assertThat(f.support()).isEqualTo(FilterSupport.FULL)
    assertThat(f.aql("d")).isEqualTo("""`d`.`n.a.m.e.`.`first` == "fname"""")
  }

}
