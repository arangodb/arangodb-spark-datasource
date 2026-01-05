package org.apache.spark.sql.arangodb.datasource

import com.arangodb.model.OverwriteMode
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest.arangoDatasource
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.sql.{Date, Timestamp}

class ReadWriteDataTypeTest extends BaseSparkTest {

  private val collectionName = "datatypes"
  private val data: Seq[Row] = Seq(
    Row(
      false,
      1.1,
      0.09375f,
      1,
      1L,
      Date.valueOf("2021-01-01"),
      Timestamp.valueOf("2021-01-01 01:01:01.111"),
      1.toShort,
      1.toByte,
      "one",
      Array(1, 1, 1),
      Array(Array("a", "b", "c"), Array("d", "e", "f")),
      Map("a" -> 1, "b" -> 1),
      Row("a1", 1),
      BigDecimal(Long.MaxValue) + .5
    ),
    Row(
      true,
      2.2,
      2.2f,
      2,
      2L,
      Date.valueOf("2022-02-02"),
      Timestamp.valueOf("2022-02-02 02:02:02.222"),
      2.toShort,
      2.toByte,
      "two",
      Array(2, 2, 2),
      Array(Array("a", "b", "c"), Array("d", "e", "f")),
      Map("a" -> 2, "b" -> 2),
      Row("a2", 2),
      BigDecimal(Long.MinValue) - 1
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


  @Test
  def writeDecimalTypeWithJsonContentTypeShouldThrow(): Unit = {
    val schemaWithDecimal = schema.add(StructField("decimal", DecimalType(38, 18), nullable = false))
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schemaWithDecimal)
    //noinspection ConvertExpressionToSAM
    val thrown = catchThrowable(new ThrowingCallable {
      override def call(): Unit = write(df, "http", "json")
    })
    assertThat(thrown).isInstanceOf(classOf[UnsupportedOperationException])
    assertThat(thrown).hasMessageContaining("Cannot write DecimalType when using contentType=json")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def roundTripReadWrite(protocol: String, contentType: String): Unit = {
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    doRoundTripReadWrite(df, protocol, contentType)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def roundTripReadWriteDecimalType(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(contentType == "vpack")

    val schemaWithDecimal = schema.add(StructField("decimal", DecimalType(38, 18), nullable = false))
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schemaWithDecimal)
    doRoundTripReadWrite(df, protocol, contentType)
  }

  def write(df: DataFrame, protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue,
        ArangoDBConf.CONFIRM_TRUNCATE -> "true"
      ))
      .save()
  }

  def doRoundTripReadWrite(df: DataFrame, protocol: String, contentType: String): Unit = {
    val initial = df.collect()
      .map(it => it.getValuesMap(it.schema.fieldNames))

    write(df, protocol, contentType)
    val read = spark.read
      .format(arangoDatasource)
      .options(options + ("table" -> collectionName))
      .schema(df.schema)
      .load()
      .collect()
      .map(it => it.getValuesMap(it.schema.fieldNames))

    assertThat(read).containsExactlyInAnyOrder(initial: _*)
  }

}

