package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.{SPARK_VERSION, SparkException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.catalyst.util.{BadRecordException, DropMalformedMode, FailFastMode, ParseMode}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
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
    // PERMISSIVE
    doTestBadRecord(schema, data, jsonData, Map(ArangoDBConf.CONTENT_TYPE -> contentType))

    // PERMISSIVE with columnNameOfCorruptRecord
    doTestBadRecord(
      schema.add(StructField("corruptRecord", StringType)),
      data,
      jsonData,
      Map(
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.COLUMN_NAME_OF_CORRUPT_RECORD -> "corruptRecord"
      )
    )

    // DROPMALFORMED
    doTestBadRecord(schema, data, jsonData,
      Map(
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.PARSE_MODE -> DropMalformedMode.name
      )
    )

    // FAILFAST
    val df = BaseSparkTest.createDF(collectionName, data, schema, Map(
      ArangoDBConf.CONTENT_TYPE -> contentType,
      ArangoDBConf.PARSE_MODE -> FailFastMode.name
    ))
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.collect()
    })

    assertThat(thrown.getCause).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause).hasMessageContaining("Malformed record")
    if (!SPARK_VERSION.startsWith("2.4")) { // [SPARK-25886]
      assertThat(thrown.getCause).hasCauseInstanceOf(classOf[BadRecordException])
    }
  }

  private def doTestBadRecord(
                               schema: StructType,
                               data: Iterable[Map[String, Any]],
                               jsonData: Seq[String],
                               opts: Map[String, String] = Map.empty
                             ) = {
    import spark.implicits._
    val dfFromJson: DataFrame = spark.read.schema(schema).options(opts).json(jsonData.toDS)
    dfFromJson.show()

    val tableDF = BaseSparkTest.createDF(collectionName, data, schema, opts)
    assertThat(tableDF.collect()).isEqualTo(dfFromJson.collect())

    val queryDF = BaseSparkTest.createQueryDF(s"RETURN ${jsonData.head}", schema, opts)
    assertThat(queryDF.collect()).isEqualTo(dfFromJson.collect())
  }

}
