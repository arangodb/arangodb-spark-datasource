import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, not}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import scala.collection.JavaConverters._

class CompositeFilterTest extends BaseSparkTest {
  private val df = CompositeFilterTest.df

  @Test
  def orFilter(): Unit = {
    val fieldName = "integer"
    val value = CompositeFilterTest.data.head(fieldName)
    val res = df.filter(col(fieldName).equalTo(0) or col(fieldName).equalTo(1)).collect()
      .map(_.getValuesMap[Any](CompositeFilterTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM compositeFilter
         |WHERE $fieldName = 0 OR $fieldName = 1
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](CompositeFilterTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def notFilter(): Unit = {
    val fieldName = "integer"
    val value = CompositeFilterTest.data.head(fieldName)
    val res = df.filter(not(col(fieldName).equalTo(2))).collect()
      .map(_.getValuesMap[Any](CompositeFilterTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM compositeFilter
         |WHERE NOT ($fieldName = 2)
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](CompositeFilterTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def orAndFilter(): Unit = {
    val fieldName1 = "integer"
    val value1 = CompositeFilterTest.data.head(fieldName1)

    val fieldName2 = "string"
    val value2 = CompositeFilterTest.data.head(fieldName2)

    val res = df.filter(col("bool").equalTo(false) or (col(fieldName1).equalTo(value1) and col(fieldName2).equalTo(value2))).collect()
      .map(_.getValuesMap[Any](CompositeFilterTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName1)).isEqualTo(value1)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM compositeFilter
         |WHERE bool = false OR ($fieldName1 = $value1 AND $fieldName2 = "$value2")
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](CompositeFilterTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName1)).isEqualTo(value1)
  }

}

object CompositeFilterTest {
  private var df: DataFrame = _
  private val data: Seq[Map[String, Any]] = Seq(
    Map(
      "integer" -> 1,
      "string" -> "one",
      "bool" -> true
    ),
    Map(
      "integer" -> 2,
      "string" -> "two",
      "bool" -> true
    )
  )

  private val schema = StructType(Array(
    // atomic types
    StructField("integer", IntegerType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructField("bool", BooleanType, nullable = false)
  ))

  @BeforeAll
  def init(): Unit = {
    df = BaseSparkTest.createDF("compositeFilter", data.asInstanceOf[Seq[Any]].asJava, schema)
  }

  @AfterAll
  def cleanup(): Unit = {
    BaseSparkTest.dropTable("compositeFilter")
  }
}