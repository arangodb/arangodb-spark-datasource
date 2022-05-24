package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.model.{AqlQueryOptions, OverwriteMode}
import org.apache.spark.sql.arangodb.commons.exceptions.{ArangoDBDataWriterException, ArangoDBMultiException, DataWriteAbortException}
import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, CollectionType}
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SPARK_VERSION, SparkException}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.JavaConverters._

class AbortTest extends BaseSparkTest {

  private val collectionName = "abortTest"
  private val collection: ArangoCollection = db.collection(collectionName)

  private val rows = Seq(
    Row("k1", "invalidFrom", "invalidFrom", "to/to"),
    Row("k2", "invalidFrom", "invalidFrom", "to/to"),
    Row("k3", "invalidFrom", "invalidFrom", "to/to"),
    Row("k4", "invalidFrom", "invalidFrom", "to/to"),
    Row("k5", "invalidFrom", "invalidFrom", "to/to"),
    Row("k6", "invalidFrom", "invalidFrom", "to/to"),
    Row("k7", "invalidFrom", "invalidFrom", "to/to"),
    Row("???", "invalidKey", "from/from", "to/to"),
    Row("valid", "valid", "from/from", "to/to")
  )

  private val df = spark.createDataFrame(rows.asJava, StructType(Array(
    StructField("_key", StringType, nullable = false),
    StructField("name", StringType),
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
  def dfWithoutKeyFieldShouldNotRetry(protocol: String, contentType: String): Unit = {
    val dfWithoutKey = df.repartition(1).withColumnRenamed("_key", "key")
    shouldNotRetry(dfWithoutKey, protocol, contentType)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def dfWithNullableKeyFieldShouldNotRetry(protocol: String, contentType: String): Unit = {
    // FIXME: https://arangodb.atlassian.net/browse/BTS-615
    assumeTrue(isSingle)

    val nullableKeySchema = StructType(df.schema.map(p =>
      if (p.name == "_key") StructField(p.name, p.dataType)
      else p
    ))
    val dfWithoutKey = spark.createDataFrame(df.rdd, nullableKeySchema)
    shouldNotRetry(dfWithoutKey, protocol, contentType)
  }

  private def shouldNotRetry(notRetryableDF: DataFrame, protocol: String, contentType: String): Unit = {
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = notRetryableDF.repartition(1).write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Append)
        .options(options + (
          ArangoDBConf.COLLECTION -> collectionName,
          ArangoDBConf.PROTOCOL -> protocol,
          ArangoDBConf.CONTENT_TYPE -> contentType,
          ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue,
          ArangoDBConf.COLLECTION_TYPE -> CollectionType.EDGE.name
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBDataWriterException])
    assertThat(thrown.getCause.getCause.asInstanceOf[ArangoDBDataWriterException].attempts).isEqualTo(1)
    assertThat(thrown.getCause.getCause.getMessage).contains("Failed 1 times, most recent failure:")

    val validInserted = db.query(
      s"""FOR d IN $collectionName FILTER d.name == "valid" RETURN d""",
      new AqlQueryOptions().count(true),
      classOf[Int]
    ).getCount

    assertThat(validInserted).isEqualTo(1)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def multipleErrors(protocol: String, contentType: String): Unit = {
    // FIXME: https://arangodb.atlassian.net/browse/BTS-615
    assumeTrue(isSingle)

    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.repartition(1).write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Append)
        .options(options + (
          ArangoDBConf.COLLECTION -> collectionName,
          ArangoDBConf.PROTOCOL -> protocol,
          ArangoDBConf.CONTENT_TYPE -> contentType,
          ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue,
          ArangoDBConf.COLLECTION_TYPE -> CollectionType.EDGE.name
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBDataWriterException])
    assertThat(thrown.getCause.getCause.asInstanceOf[ArangoDBDataWriterException].attempts).isEqualTo(10)
    assertThat(thrown.getCause.getCause.getMessage).contains("Failed 10 times, most recent failure:")

    val rootEx = thrown.getCause.getCause.getCause
    assertThat(rootEx).isInstanceOf(classOf[ArangoDBMultiException])
    val eMessage = rootEx.getMessage
    assertThat(eMessage).contains("""Error: 1233 - edge attribute missing or invalid for record: {"_key":"k1","name":"invalidFrom","_from":"invalidFrom","_to":"to/to"}""")
    assertThat(eMessage).contains("""Error: 1233 - edge attribute missing or invalid for record: {"_key":"k2","name":"invalidFrom","_from":"invalidFrom","_to":"to/to"}""")
    assertThat(eMessage).contains("""Error: 1233 - edge attribute missing or invalid for record: {"_key":"k3","name":"invalidFrom","_from":"invalidFrom","_to":"to/to"}""")
    assertThat(eMessage).contains("""Error: 1233 - edge attribute missing or invalid for record: {"_key":"k4","name":"invalidFrom","_from":"invalidFrom","_to":"to/to"}""")
    assertThat(eMessage).contains("""Error: 1233 - edge attribute missing or invalid for record: {"_key":"k5","name":"invalidFrom","_from":"invalidFrom","_to":"to/to"}""")
    assertThat(eMessage).contains("""Error: 1221 - illegal document key for record: {"_key":"???","name":"invalidKey","_from":"from/from","_to":"to/to"}""")
    assertThat(eMessage).doesNotContain("k6")
    assertThat(eMessage).doesNotContain("k7")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeAppend(protocol: String, contentType: String): Unit = {
    // FIXME: https://arangodb.atlassian.net/browse/BTS-615
    assumeTrue(isSingle)

    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Append)
        .options(options + (
          ArangoDBConf.COLLECTION -> collectionName,
          ArangoDBConf.PROTOCOL -> protocol,
          ArangoDBConf.CONTENT_TYPE -> contentType,
          ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBDataWriterException])
    val rootEx = thrown.getCause.getCause.getCause
    assertThat(rootEx).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = rootEx.asInstanceOf[ArangoDBMultiException].errors
    assertThat(errors.length).isEqualTo(1)
    assertThat(errors.head._1.getErrorNum).isEqualTo(1221)
    assertThat(errors.head._2).contains("\"_key\":\"???\"")
    assertThat(thrown.getCause.getSuppressed.head).isInstanceOf(classOf[DataWriteAbortException])
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwrite(protocol: String, contentType: String): Unit = {
    // FIXME: https://arangodb.atlassian.net/browse/BTS-615
    assumeTrue(isSingle)

    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Overwrite)
        .options(options + (
          ArangoDBConf.COLLECTION -> collectionName,
          ArangoDBConf.PROTOCOL -> protocol,
          ArangoDBConf.CONTENT_TYPE -> contentType,
          ArangoDBConf.CONFIRM_TRUNCATE -> "true",
          ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBDataWriterException])
    val rootEx = thrown.getCause.getCause.getCause
    assertThat(rootEx).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = rootEx.asInstanceOf[ArangoDBMultiException].errors
    assertThat(errors.length).isEqualTo(1)
    assertThat(errors.head._1.getErrorNum).isEqualTo(1221)
    assertThat(errors.head._2).contains("\"_key\":\"???\"")
    assertThat(collection.count().getCount).isEqualTo(0L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeErrorIfExists(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(SPARK_VERSION.startsWith("2.4"))
    // FIXME: https://arangodb.atlassian.net/browse/BTS-615
    assumeTrue(isSingle)

    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.ErrorIfExists)
        .options(options + (
          ArangoDBConf.COLLECTION -> collectionName,
          ArangoDBConf.PROTOCOL -> protocol,
          ArangoDBConf.CONTENT_TYPE -> contentType,
          ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBDataWriterException])
    val rootEx = thrown.getCause.getCause.getCause
    assertThat(rootEx).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = rootEx.asInstanceOf[ArangoDBMultiException].errors
    assertThat(errors.length).isEqualTo(1)
    assertThat(errors.head._1.getErrorNum).isEqualTo(1221)
    assertThat(errors.head._2).contains("\"_key\":\"???\"")
    assertThat(collection.exists()).isFalse
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeIgnore(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(SPARK_VERSION.startsWith("2.4"))
    // FIXME: https://arangodb.atlassian.net/browse/BTS-615
    assumeTrue(isSingle)

    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Ignore)
        .options(options + (
          ArangoDBConf.COLLECTION -> collectionName,
          ArangoDBConf.PROTOCOL -> protocol,
          ArangoDBConf.CONTENT_TYPE -> contentType,
          ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBDataWriterException])
    val rootEx = thrown.getCause.getCause.getCause
    assertThat(rootEx).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = rootEx.asInstanceOf[ArangoDBMultiException].errors
    assertThat(errors.length).isEqualTo(1)
    assertThat(errors.head._1.getErrorNum).isEqualTo(1221)
    assertThat(errors.head._2).contains("\"_key\":\"???\"")
    assertThat(collection.exists()).isFalse
  }

}
