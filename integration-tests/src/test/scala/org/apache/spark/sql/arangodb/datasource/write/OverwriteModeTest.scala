package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.entity.BaseDocument
import org.apache.spark.SparkException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


class OverwriteModeTest extends BaseSparkTest {

  private val collectionName = "chessPlayersOverwriteMode"
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
      ("Radjabov", "Teimour")
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
  def overwriteModeConflictWithExistingDocument(protocol: String, contentType: String): Unit = {
    collection.create()
    collection.insertDocument(new BaseDocument("Carlsen"))
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format(BaseSparkTest.arangoDatasource)
        .mode(SaveMode.Append)
        .options(options + (
          ArangoOptions.COLLECTION -> collectionName,
          ArangoOptions.PROTOCOL -> protocol,
          ArangoOptions.CONTENT_TYPE -> contentType,
          ArangoOptions.OVERWRITE_MODE -> "conflict"
        ))
        .save()
    })
    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause).isInstanceOf(classOf[SparkException]) // executor exception
    val rootEx = thrown.getCause.getCause
    assertThat(rootEx).isInstanceOf(classOf[ArangoDBMultiException])
    assertThat(rootEx.asInstanceOf[ArangoDBMultiException]).hasMessageContaining("conflicting key: Carlsen")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def overwriteModeIgnore(protocol: String, contentType: String): Unit = {
    collection.create()
    val doc = new BaseDocument("Carlsen")
    doc.addAttribute("name", "M.")
    collection.insertDocument(doc)

    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.OVERWRITE_MODE -> "ignore"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
    val c = collection.getDocument("Carlsen", classOf[BaseDocument])
    assertThat(c.getAttribute("name")).isEqualTo("M.")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def overwriteModeReplace(protocol: String, contentType: String): Unit = {
    collection.create()
    val doc = new BaseDocument("Carlsen")
    doc.addAttribute("name", "M.")
    collection.insertDocument(doc)

    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.OVERWRITE_MODE -> "replace"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
    val c = collection.getDocument("Carlsen", classOf[BaseDocument])
    assertThat(c.getAttribute("name")).isEqualTo("Magnus")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def overwriteModeUpdate(protocol: String, contentType: String): Unit = {
    collection.create()
    val doc = new BaseDocument("Carlsen")
    doc.addAttribute("byear", 1990)
    collection.insertDocument(doc)

    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.OVERWRITE_MODE -> "update"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
    val c = collection.getDocument("Carlsen", classOf[BaseDocument])
    assertThat(c.getAttribute("name")).isEqualTo("Magnus")
    assertThat(c.getAttribute("byear")).isEqualTo(1990)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def overwriteModeUpdateKeepNullTrue(protocol: String, contentType: String): Unit = {
    collection.create()
    val doc = new BaseDocument("Carlsen")
    doc.addAttribute("name", "Magnus")
    collection.insertDocument(doc)

    Seq(("Carlsen", null)).toDF("_key", "name")
      .write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.OVERWRITE_MODE -> "update",
        ArangoOptions.KEEP_NULL -> "true"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(1L)
    val c = collection.getDocument("Carlsen", classOf[BaseDocument])
    assertThat(c.getProperties.containsKey("name")).isTrue
    assertThat(c.getProperties.get("name")).isNull()
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def overwriteModeUpdateKeepNullFalse(protocol: String, contentType: String): Unit = {
    collection.create()
    val doc = new BaseDocument("Carlsen")
    doc.addAttribute("name", "Magnus")
    collection.insertDocument(doc)

    Seq(("Carlsen", null)).toDF("_key", "name")
      .write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.OVERWRITE_MODE -> "update",
        ArangoOptions.KEEP_NULL -> "false"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(1L)
    val c = collection.getDocument("Carlsen", classOf[BaseDocument])
    assertThat(c.getProperties.containsKey("name")).isFalse
  }


}
