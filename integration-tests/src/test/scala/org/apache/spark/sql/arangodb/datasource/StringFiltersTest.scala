package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import scala.collection.JavaConverters._

class StringFiltersTest extends BaseSparkTest {
  private val df = StringFiltersTest.df

  @Test
  def startsWith(): Unit = {
    val fieldName = "string"
    val value = StringFiltersTest.data.head(fieldName).asInstanceOf[String]
    val res = df.filter(col(fieldName).startsWith("Lorem")).collect()
      .map(_.getValuesMap[Any](StringFiltersTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM stringFilters
         |WHERE $fieldName LIKE "Lorem%"
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](StringFiltersTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def endsWith(): Unit = {
    val fieldName = "string"
    val value = StringFiltersTest.data.head(fieldName).asInstanceOf[String]
    val res = df.filter(col(fieldName).endsWith("amet")).collect()
      .map(_.getValuesMap[Any](StringFiltersTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM stringFilters
         |WHERE $fieldName LIKE "%amet"
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](StringFiltersTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def contains(): Unit = {
    val fieldName = "string"
    val value = StringFiltersTest.data.head(fieldName).asInstanceOf[String]
    val res = df.filter(col(fieldName).contains("dolor")).collect()
      .map(_.getValuesMap[Any](StringFiltersTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM stringFilters
         |WHERE $fieldName LIKE "%dolor%"
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](StringFiltersTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

}

object StringFiltersTest {
  private var df: DataFrame = _
  private val data: Seq[Map[String, Any]] = Seq(
    Map(
      "string" -> "Lorem ipsum dolor sit amet"
    ),
    Map(
      "string" -> "consectetur adipiscing elit"
    )
  )

  private val schema = StructType(Array(
    StructField("string", StringType, nullable = false)
  ))

  @BeforeAll
  def init(): Unit = {
    df = BaseSparkTest.createDF("stringFilters", data.asInstanceOf[Seq[Any]].asJava, schema)
  }

  @AfterAll
  def cleanup(): Unit = {
    BaseSparkTest.dropTable("stringFilters")
  }
}