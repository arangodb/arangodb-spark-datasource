package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class BadRecordsTest extends BaseSparkTest {
  private val collectionName = "deserializationCast"

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def stringAsInteger(contentType: String): Unit = testBadRecord(
    StructType(Array(StructField("a", IntegerType))),
    Seq(Map("a" -> "1")),
    Seq("""{"a":"1"}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def booleanAsInteger(contentType: String): Unit = testBadRecord(
    StructType(Array(StructField("a", IntegerType))),
    Seq(Map("a" -> true)),
    Seq("""{"a":true}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def stringAsDouble(contentType: String): Unit = testBadRecord(
    StructType(Array(StructField("a", DoubleType))),
    Seq(Map("a" -> "1")),
    Seq("""{"a":"1"}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def booleanAsDouble(contentType: String): Unit = testBadRecord(
    StructType(Array(StructField("a", DoubleType))),
    Seq(Map("a" -> true)),
    Seq("""{"a":true}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def stringAsBoolean(contentType: String): Unit = testBadRecord(
    StructType(Array(StructField("a", BooleanType))),
    Seq(Map("a" -> "true")),
    Seq("""{"a":"true"}"""),
    contentType
  )

  @ParameterizedTest
  @ValueSource(strings = Array("vpack", "json"))
  def numberAsBoolean(contentType: String): Unit = testBadRecord(
    StructType(Array(StructField("a", BooleanType))),
    Seq(Map("a" -> 1)),
    Seq("""{"a":1}"""),
    contentType
  )

  private def testBadRecord(
                             schema: StructType,
                             data: Iterable[Map[String, Any]],
                             jsonData: Seq[String],
                             contentType: String
                           ) = {
    doTestBadRecord(schema, data, jsonData, Map(ArangoOptions.CONTENT_TYPE -> contentType))
    doTestBadRecord(
      schema.add(StructField("columnNameOfCorruptRecord", StringType)), data, jsonData,
      Map(
        ArangoOptions.CONTENT_TYPE -> contentType,
        "columnNameOfCorruptRecord" -> "columnNameOfCorruptRecord"
      )
    )
  }

  private def doTestBadRecord(
                               schema: StructType,
                               data: Iterable[Map[String, Any]],
                               jsonData: Seq[String],
                               options: Map[String, String] = Map.empty
                             ) = {
    import spark.implicits._
    val dfFromJson: DataFrame = spark.read.schema(schema).options(options).json(jsonData.toDS)
    dfFromJson.show()
    val df = BaseSparkTest.createDF(collectionName, data, schema, options)
    assertThat(df.collect()).isEqualTo(dfFromJson.collect())
  }

}
