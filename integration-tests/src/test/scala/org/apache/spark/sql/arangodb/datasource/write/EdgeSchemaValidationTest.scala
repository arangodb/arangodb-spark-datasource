package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, CollectionType}
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.jdk.CollectionConverters.seqAsJavaListConverter

class EdgeSchemaValidationTest extends BaseSparkTest {

  private val collectionName = "edgeSchemaValidationTest"
  private val collection: ArangoCollection = db.collection(collectionName)

  private val rows = Seq(
    Row("k1", "from/from", "to/to"),
    Row("k2", "from/from", "to/to"),
    Row("k3", "from/from", "to/to")
  )

  private val df = spark.createDataFrame(rows.asJava, StructType(Array(
    StructField("_key", StringType, nullable = false),
    StructField("_from", StringType, nullable = false),
    StructField("_to", StringType, nullable = false)
  )))

  @BeforeEach
  def beforeEach(): Unit = {
    if (collection.exists()) {
      collection.drop()
    }
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def write(protocol: String, contentType: String): Unit = {
    doWrite(df, protocol, contentType)
    assertThat(collection.count().getCount).isEqualTo(3L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def dfWithNullableFromFieldShouldFail(protocol: String, contentType: String): Unit = {
    val nullableFromSchema = StructType(df.schema.map(p =>
      if (p.name == "_from") StructField(p.name, p.dataType)
      else p
    ))
    val dfWithNullableFrom = spark.createDataFrame(df.rdd, nullableFromSchema)
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = doWrite(dfWithNullableFrom, protocol, contentType)
    })

    assertThat(thrown).isInstanceOf(classOf[IllegalArgumentException])
    assertThat(thrown).hasMessageContaining("_from")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def dfWithNullableToFieldShouldFail(protocol: String, contentType: String): Unit = {
    val nullableFromSchema = StructType(df.schema.map(p =>
      if (p.name == "_to") StructField(p.name, p.dataType)
      else p
    ))
    val dfWithNullableFrom = spark.createDataFrame(df.rdd, nullableFromSchema)
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = doWrite(dfWithNullableFrom, protocol, contentType)
    })

    assertThat(thrown).isInstanceOf(classOf[IllegalArgumentException])
    assertThat(thrown).hasMessageContaining("_to")
  }

  private def doWrite(testDF: DataFrame, protocol: String, contentType: String): Unit = {
    testDF.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.COLLECTION_TYPE -> CollectionType.EDGE.name
      ))
      .save()
  }

}
