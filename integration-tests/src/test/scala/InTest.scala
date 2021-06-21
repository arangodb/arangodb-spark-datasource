import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import scala.collection.mutable

class InTest extends BaseSparkTest {
  private val df = InTest.df

  @Test
  def bool(): Unit = {
    val fieldName = "bool"
    val value = InTest.data.head(fieldName)
    val res = df.filter(col(fieldName).isin(true, false)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(2)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN (true, false)
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(2)
  }

  @Test
  def integer(): Unit = {
    val fieldName = "integer"
    val value = InTest.data.head(fieldName)
    val res = df.filter(col(fieldName).isin(0, 1)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN (0, 1)
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def date(): Unit = {
    val fieldName = "date"
    val value = InTest.data.head(fieldName)
    val value2 = Date.valueOf("2020-01-01")
    val res = df.filter(col(fieldName).isin(value, value2)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN ("$value", "$value2")
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def timestampString(): Unit = {
    val fieldName = "timestampString"
    val value = InTest.data.head(fieldName)
    val value2 = Timestamp.valueOf("2020-01-01 00:00:00.000")
    val res = df.filter(col(fieldName).isin(value, value2)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN ("$value", "$value2")
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def timestampMillis(): Unit = {
    val fieldName = "timestampMillis"
    val value = Timestamp.valueOf("2021-01-01 01:01:01.111")
    val value2 = Timestamp.valueOf("2020-01-01 00:00:00.000")
    val res = df.filter(col(fieldName).isin(value, value2)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN ("$value", "$value2")
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def string(): Unit = {
    val fieldName = "string"
    val value = InTest.data.head(fieldName)
    val value2 = "foo"
    val res = df.filter(col(fieldName).isin(value, value2)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName)).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN ("$value", "$value2")
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

  @Test
  def intArray(): Unit = {
    val fieldName = "intArray"
    val value = InTest.data.head(fieldName).asInstanceOf[Array[Int]]
    val value2 = Array(4, 5, 6)
    val res = df.filter(col(fieldName).isin(value, value2)).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(res).hasSize(1)
    assertThat(res.head(fieldName).asInstanceOf[mutable.WrappedArray[Int]].toArray).isEqualTo(value)
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN (array(${value.mkString(",")}), array(${value2.mkString(",")}))
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName).asInstanceOf[mutable.WrappedArray[Int]].toArray).isEqualTo(value)
  }

  @Test
  def struct(): Unit = {
    val fieldName = "struct"
    val value = new GenericRowWithSchema(
      Array("a1", 1),
      StructType(Array(
        StructField("a", StringType),
        StructField("b", IntegerType)
      ))
    )
    val sqlRes = spark.sql(
      s"""
         |SELECT * FROM in
         |WHERE $fieldName IN (("foo" AS a, 9 AS b), ("a1" AS a, 1 AS b))
         |""".stripMargin).collect()
      .map(_.getValuesMap[Any](InTest.schema.fieldNames))
    assertThat(sqlRes).hasSize(1)
    assertThat(sqlRes.head(fieldName)).isEqualTo(value)
  }

}

object InTest {
  private var df: DataFrame = _
  private val data: Seq[Map[String, Any]] = Seq(
    Map(
      "bool" -> false,
      "integer" -> 1,
      "date" -> Date.valueOf("2021-01-01"),
      "timestampString" -> Timestamp.valueOf("2021-01-01 01:01:01.111"),
      "timestampMillis" -> Timestamp.valueOf("2021-01-01 01:01:01.111").getTime,
      "string" -> "one",
      "intArray" -> Array(1, 1, 1),
      "struct" -> Map(
        "a" -> "a1",
        "b" -> 1
      )
    ),
    Map(
      "bool" -> true,
      "integer" -> 2,
      "date" -> Date.valueOf("2022-02-02"),
      "timestampString" -> Timestamp.valueOf("2022-02-02 02:02:02.222"),
      "timestampMillis" -> Timestamp.valueOf("2022-02-02 02:02:02.222").getTime,
      "string" -> "two",
      "intArray" -> Array(2, 2, 2),
      "struct" -> Map(
        "a" -> "a2",
        "b" -> 2
      )
    )
  )

  private val schema = StructType(Array(
    // atomic types
    StructField("bool", BooleanType, nullable = false),
    StructField("integer", IntegerType, nullable = false),
    StructField("date", DateType, nullable = false),
    StructField("timestampString", TimestampType, nullable = false),
    StructField("timestampMillis", TimestampType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructField("intArray", ArrayType(IntegerType), nullable = false),
    StructField("struct", StructType(Array(
      StructField("a", StringType),
      StructField("b", IntegerType)
    )))
  ))

  @BeforeAll
  def init(): Unit = {
    df = BaseSparkTest.createDF("in", data.asInstanceOf[Seq[Any]].asJava, schema)
  }

  @AfterAll
  def cleanup(): Unit = {
    BaseSparkTest.dropTable("in")
  }
}