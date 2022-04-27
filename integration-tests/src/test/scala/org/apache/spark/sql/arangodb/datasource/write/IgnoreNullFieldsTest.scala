package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.entity.BaseDocument
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.jdk.CollectionConverters.mapAsJavaMapConverter


class IgnoreNullFieldsTest extends BaseSparkTest {

  private val collectionName = "ignoreNullFields"
  private val collection: ArangoCollection = db.collection(collectionName)

  import spark.implicits._

  @BeforeEach
  def beforeEach(): Unit = {
    if (!collection.exists()) {
      collection.create()
    } else {
      collection.truncate()
    }
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def ignoreNullFieldsTrue(protocol: String, contentType: String): Unit = {
    Seq(
      ("Carlsen", "Magnus"),
      ("Caruana", null)
    )
      .toDF("_key", "name")
      .write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.IGNORE_NULL_FIELDS -> "true"
      ))
      .save()

    assertThat(collection.getDocument("Carlsen", classOf[BaseDocument]).getProperties).containsEntry("name", "Magnus")
    assertThat(collection.getDocument("Caruana", classOf[BaseDocument]).getProperties).doesNotContainKey("name")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def ignoreNullFieldsFalse(protocol: String, contentType: String): Unit = {
    Seq(
      ("Carlsen", "Magnus"),
      ("Caruana", null)
    )
      .toDF("_key", "name")
      .write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.IGNORE_NULL_FIELDS -> "false"
      ))
      .save()

    assertThat(collection.getDocument("Carlsen", classOf[BaseDocument]).getProperties).containsEntry("name", "Magnus")
    assertThat(collection.getDocument("Caruana", classOf[BaseDocument]).getProperties).containsEntry("name", null)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def ignoreNullFieldsDefault(protocol: String, contentType: String): Unit = {
    Seq(
      ("Carlsen", "Magnus"),
      ("Caruana", null)
    )
      .toDF("_key", "name")
      .write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType
      ))
      .save()

    assertThat(collection.getDocument("Carlsen", classOf[BaseDocument]).getProperties).containsEntry("name", "Magnus")
    assertThat(collection.getDocument("Caruana", classOf[BaseDocument]).getProperties).containsEntry("name", null)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def nestedStructType(protocol: String, contentType: String): Unit = {
    val data = Seq(
      Row("Carlsen", Row("Magnus")),
      Row("Caruana", Row(null))
    )

    val schema = StructType(Array(
      StructField("_key", StringType),
      StructField("info", StructType(Array(
        StructField("name", StringType)
      )))
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.printSchema()
    df.show()
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.IGNORE_NULL_FIELDS -> "true"
      ))
      .save()

    assertThat(getInfo("Carlsen")).containsEntry("name", "Magnus")
    assertThat(getInfo("Caruana")).doesNotContainKey("name")
  }

  private def getInfo(key: String) = collection.getDocument(key, classOf[BaseDocument])
    .getAttribute("info")
    .asInstanceOf[Map[String, String]]
    .asJava

}
