package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * FIXME: many vpack tests fail
 */
@Disabled
class DeserializationCastTest extends BaseSparkTest {
  private val collectionName = "deserializationCast"

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def numberIntToStringCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> 1)),
    Seq("""{"a":1}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def numberDecToStringCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> 1.1)),
    Seq("""{"a":1.1}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def boolToStringCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> true)),
    Seq("""{"a":true}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def objectToStringCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> Map("b" -> "c"))),
    Seq("""{"a":{"b":"c"}}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def arrayToStringCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", StringType))),
    Seq(Map("a" -> Array(1, 2))),
    Seq("""{"a":[1,2]}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def nullToIntegerCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", IntegerType, nullable = false))),
    Seq(Map("a" -> null)),
    Seq("""{"a":null}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def nullToDoubleCast(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", DoubleType, nullable = false))),
    Seq(Map("a" -> null)),
    Seq("""{"a":null}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def nullAsBoolean(contentType: String): Unit = doTestBadRecordImplicitCast(
    StructType(Array(StructField("a", BooleanType, nullable = false))),
    Seq(Map("a" -> null)),
    Seq("""{"a":null}"""),
    contentType
  )

  private def doTestBadRecordImplicitCast(
                                           schema: StructType,
                                           data: Iterable[Map[String, Any]],
                                           jsonData: Seq[String],
                                           contentType: String
                                         ) = {
    import spark.implicits._
    val dfFromJson: DataFrame = spark.read.schema(schema).json(jsonData.toDS)
    dfFromJson.show()
    val df = BaseSparkTest.createDF(collectionName, data, schema, contentType)
    assertThat(df.collect()).isEqualTo(dfFromJson.collect())
  }
}
