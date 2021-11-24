package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest.arangoDatasource
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.sql.{Date, Timestamp}

class ReadWriteDataTypeTest extends BaseSparkTest {

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def roundTripReadWrite(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(contentType != "json")

    val firstRead = ReadWriteDataTypeTest.df.collect()
      .map(it => it.getValuesMap(it.schema.fieldNames))

    ReadWriteDataTypeTest.df.show()
    ReadWriteDataTypeTest.df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoOptions.COLLECTION -> (ReadWriteDataTypeTest.collectionName + "_2"),
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.OVERWRITE_MODE -> "replace",
        ArangoOptions.CONFIRM_TRUNCATE -> "true"
      ))
      .save()

    val secondRead = spark.read
      .format(arangoDatasource)
      .options(options + ("table" -> ReadWriteDataTypeTest.collectionName))
      .schema(ReadWriteDataTypeTest.schema)
      .load()
      .collect()
      .map(it => it.getValuesMap(it.schema.fieldNames))

    assertThat(secondRead).isEqualTo(firstRead)
  }

}

object ReadWriteDataTypeTest {
  private val collectionName = "datatypes"
  private var df: DataFrame = _
  private val data: Seq[Map[String, Any]] = Seq(
    Map(
      "bool" -> false,
      "double" -> 1.1,
      "float" -> 0.09375f,
      "integer" -> 1,
      "long" -> 1L,
      "date" -> Date.valueOf("2021-01-01"),
      "timestamp" -> Timestamp.valueOf("2021-01-01 01:01:01.111"),
      "short" -> 1.toShort,
      "byte" -> 1.toByte,
      "string" -> "one",
      "intArray" -> Array(1, 1, 1),
      "stringArrayArray" -> Array(Array("a", "b", "c"), Array("d", "e", "f")),
      "intMap" -> Map("a" -> 1, "b" -> 1),
      "struct" -> Map(
        "a" -> "a1",
        "b" -> 1
      )
    ),
    Map(
      "bool" -> true,
      "double" -> 2.2,
      "float" -> 2.2f,
      "integer" -> 2,
      "long" -> 2L,
      "date" -> Date.valueOf("2022-02-02"),
      "timestamp" -> Timestamp.valueOf("2022-02-02 02:02:02.222"),
      "short" -> 2.toShort,
      "byte" -> 2.toByte,
      "string" -> "two",
      "intArray" -> Array(2, 2, 2),
      "stringArrayArray" -> Array(Array("a", "b", "c"), Array("d", "e", "f")),
      "intMap" -> Map("a" -> 2, "b" -> 2),
      "struct" -> Map(
        "a" -> "a2",
        "b" -> 2
      )
    )
  )

  private val schema = StructType(Array(
    // atomic types
    StructField("bool", BooleanType, nullable = false),
    StructField("double", DoubleType, nullable = false),
    StructField("float", FloatType, nullable = false),
    StructField("integer", IntegerType, nullable = false),
    StructField("long", LongType, nullable = false),
    StructField("date", DateType, nullable = false),
    StructField("timestamp", TimestampType, nullable = false),
    StructField("short", ShortType, nullable = false),
    StructField("byte", ByteType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructField("intArray", ArrayType(IntegerType), nullable = false),
    StructField("stringArrayArray", ArrayType(ArrayType(StringType)), nullable = false),
    StructField("intMap", MapType(StringType, IntegerType), nullable = false),
    StructField("struct", StructType(Array(
      StructField("a", StringType),
      StructField("b", IntegerType)
    )))
  ))

  @BeforeAll
  def init(): Unit = {
    df = BaseSparkTest.createDF(collectionName, data, schema)
  }

  @AfterAll
  def cleanup(): Unit = {
    BaseSparkTest.dropTable(collectionName)
  }
}
