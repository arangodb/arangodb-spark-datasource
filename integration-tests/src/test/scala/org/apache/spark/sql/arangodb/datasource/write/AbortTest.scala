package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.model.OverwriteMode
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, CollectionType}
import org.apache.spark.sql.arangodb.commons.exceptions.{ArangoDBMultiException, DataWriteAbortException}
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.{SPARK_VERSION, SparkException}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


class AbortTest extends BaseSparkTest {

  private val collectionName = "abortTest"
  private val collection: ArangoCollection = db.collection(collectionName)

  import spark.implicits._

  private val df = Seq(
    ("k1", "invalidFrom", "invalidFrom", "to/to"),
    ("k2", "invalidFrom", "invalidFrom", "to/to"),
    ("k3", "invalidFrom", "invalidFrom", "to/to"),
    ("k4", "invalidFrom", "invalidFrom", "to/to"),
    ("k5", "invalidFrom", "invalidFrom", "to/to"),
    ("k6", "invalidFrom", "invalidFrom", "to/to"),
    ("k7", "invalidFrom", "invalidFrom", "to/to"),
    ("???", "invalidKey", "from/from", "to/to")
  ).toDF("_key", "name", "_from", "_to")

  @BeforeEach
  def beforeEach(): Unit = {
    if (collection.exists()) {
      collection.drop()
    }
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
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBMultiException])
    val eMessage = thrown.getCause.getCause.getMessage
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
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = thrown.getCause.getCause.asInstanceOf[ArangoDBMultiException].errors
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
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = thrown.getCause.getCause.asInstanceOf[ArangoDBMultiException].errors
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
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = thrown.getCause.getCause.asInstanceOf[ArangoDBMultiException].errors
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
    assertThat(thrown.getCause.getCause).isInstanceOf(classOf[ArangoDBMultiException])
    val errors = thrown.getCause.getCause.asInstanceOf[ArangoDBMultiException].errors
    assertThat(errors.length).isEqualTo(1)
    assertThat(errors.head._1.getErrorNum).isEqualTo(1221)
    assertThat(errors.head._2).contains("\"_key\":\"???\"")
    assertThat(collection.exists()).isFalse
  }

}
