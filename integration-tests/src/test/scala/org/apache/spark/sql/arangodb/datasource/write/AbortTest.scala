package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.model.OverwriteMode
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
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

  private val collectionName = "chessPlayersAbort"
  private val collection: ArangoCollection = db.collection(collectionName)

  import spark.implicits._

  private val df = {
    Seq(
      ("Carlsen", "Magnus"),
      ("Caruana", "Fabiano"),
      ("Ding", "Liren"),
      ("Nepomniachtchi", "Ian"),
      ("Aronian", "Levon"),
      ("Grischuk", "Alexander"),
      ("Giri", "Anish"),
      ("Mamedyarov", "Shakhriyar"),
      ("So", "Wesley"),
      ("Radjabov", "Teimour"),
      ("???", "invalidKey")
    ).toDF("_key", "name")
      .repartition(3)
  }

  @BeforeEach
  def beforeEach(): Unit = {
    if (collection.exists()) {
      collection.drop()
    }
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
    assertThat(errors.size).isEqualTo(1)
    assertThat(errors.head.getErrorNum).isEqualTo(1221)
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
    assertThat(errors.size).isEqualTo(1)
    assertThat(errors.head.getErrorNum).isEqualTo(1221)
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
    assertThat(errors.size).isEqualTo(1)
    assertThat(errors.head.getErrorNum).isEqualTo(1221)
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
    assertThat(errors.size).isEqualTo(1)
    assertThat(errors.head.getErrorNum).isEqualTo(1221)
    assertThat(collection.exists()).isFalse
  }

}
