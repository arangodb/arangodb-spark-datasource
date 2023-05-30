package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class DeserializationCastTest extends BaseSparkTest {
  private val collectionName = "deserializationCast"

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def numberIntToStringCast(contentType: String): Unit = doTestImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> 1)),
    Seq("""{"a":1}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def numberDecToStringCast(contentType: String): Unit = doTestImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> 1.1)),
    Seq("""{"a":1.1}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def boolToStringCast(contentType: String): Unit = doTestImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> true)),
    Seq("""{"a":true}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def objectToStringCast(contentType: String): Unit = doTestImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> Map("b" -> "c"))),
    Seq("""{"a":{"b":"c"}}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def arrayToStringCast(contentType: String): Unit = doTestImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> Array(1, 2))),
    Seq("""{"a":[1,2]}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def nullToIntegerCast(contentType: String): Unit = {
    // FIXME: DE-599
    assumeTrue(!SPARK_VERSION.startsWith("3.3"))
    assumeTrue(!SPARK_VERSION.startsWith("3.4"))

    doTestImplicitCast(
      StructType(Array(StructField("a", IntegerType, nullable = false))),
      Seq(Map("a" -> null)),
      Seq("""{"a":null}"""),
      contentType
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def nullToDoubleCast(contentType: String): Unit = {
    // FIXME: DE-599
    assumeTrue(!SPARK_VERSION.startsWith("3.3"))
    assumeTrue(!SPARK_VERSION.startsWith("3.4"))

    doTestImplicitCast(
      StructType(Array(StructField("a", DoubleType, nullable = false))),
      Seq(Map("a" -> null)),
      Seq("""{"a":null}"""),
      contentType
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def nullAsBoolean(contentType: String): Unit = {
    // FIXME: DE-599
    assumeTrue(!SPARK_VERSION.startsWith("3.3"))
    assumeTrue(!SPARK_VERSION.startsWith("3.4"))

    doTestImplicitCast(
      StructType(Array(StructField("a", BooleanType, nullable = false))),
      Seq(Map("a" -> null)),
      Seq("""{"a":null}"""),
      contentType
    )
  }

  private def doTestImplicitCast(
                                  schema: StructType,
                                  data: Iterable[Map[String, Any]],
                                  jsonData: Seq[String],
                                  contentType: String
                                ) = {

    /**
     * FIXME: many vpack tests fail
     */
    assumeTrue(contentType != "vpack")

    import spark.implicits._
    val dfFromJson: DataFrame = spark.read.schema(schema).json(jsonData.toDS)
    dfFromJson.show()
    val df = BaseSparkTest.createDF(collectionName, data, schema, Map(ArangoDBConf.CONTENT_TYPE -> contentType))
    assertThat(df.collect()).isEqualTo(dfFromJson.collect())
  }
}
