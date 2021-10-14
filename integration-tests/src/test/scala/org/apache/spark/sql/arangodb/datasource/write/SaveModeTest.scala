package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


class SaveModeTest extends BaseSparkTest {

  private val collectionName = "chessPlayersSaveMode"
  private val collection: ArangoCollection = db.collection(collectionName)

  import spark.implicits._

  private val df = Seq(
    ("Carlsen", "Magnus"),
    ("Caruana", "Fabiano"),
    ("Ding", "Liren"),
    ("Nepomniachtchi", "Ian"),
    ("Aronian", "Levon"),
    ("Grischuk", "Alexander"),
    ("Giri", "Anish"),
    ("Mamedyarov", "Shakhriyar"),
    ("So", "Wesley"),
    ("Radjabov", "Teimour")
  ).toDF("surname", "name")
    .repartition(3)

  @BeforeEach
  def beforeEach(): Unit = {
    if (collection.exists()) {
      collection.drop()
    }
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeAppend(protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeAppendWithExistingCollection(protocol: String, contentType: String): Unit = {
    collection.create()
    collection.insertDocument(new Object)
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(11L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwriteShouldThrowWhenUsedAlone(protocol: String, contentType: String): Unit = {
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Overwrite)
        .options(options + (
          ArangoOptions.COLLECTION -> collectionName,
          ArangoOptions.PROTOCOL -> protocol,
          ArangoOptions.CONTENT_TYPE -> contentType
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[AnalysisException])
    assertThat(thrown.getMessage).contains(ArangoOptions.CONFIRM_TRUNCATE)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwrite(protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.CONFIRM_TRUNCATE -> "true"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwriteWithExistingCollection(protocol: String, contentType: String): Unit = {
    collection.create()
    collection.insertDocument(new Object)

    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.CONFIRM_TRUNCATE -> "true"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeErrorIfExistsShouldThrow(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(SPARK_VERSION_SHORT.startsWith("2.4"))
    collection.create()
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.ErrorIfExists)
        .options(options + (
          ArangoOptions.COLLECTION -> collectionName,
          ArangoOptions.PROTOCOL -> protocol,
          ArangoOptions.CONTENT_TYPE -> contentType
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[AnalysisException])
    assertThat(thrown.getMessage).contains("already exists")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeErrorIfExists(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(SPARK_VERSION_SHORT.startsWith("2.4"))
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.ErrorIfExists)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeIgnore(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(SPARK_VERSION_SHORT.startsWith("2.4"))
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Ignore)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeIgnoreWithExistingCollection(protocol: String, contentType: String): Unit = {
    // FIXME
    assumeTrue(SPARK_VERSION_SHORT.startsWith("2.4"))
    collection.create()
    collection.insertDocument(new Object)
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Ignore)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(1L)
  }

}
