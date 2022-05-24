package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.entity.BaseDocument
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.JavaConverters.{asJavaIterableConverter, mapAsJavaMapConverter}


class IgnoreNullFieldsTest extends BaseSparkTest {

  private val collectionName = "ignoreNullFields"
  private val collection: ArangoCollection = db.collection(collectionName)

  import spark.implicits._

  @BeforeEach
  def beforeEach(): Unit = {
    assumeTrue(SPARK_VERSION.startsWith("3"))
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

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def arrays(protocol: String, contentType: String): Unit = {
    val df = Seq(
      ("Carlsen", Array("a", "b", "c")),
      ("Caruana", Array("a", null, "c"))
    )
      .toDF("_key", "values")
    df.printSchema()
    df.show()
    df
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

    assertThat(getValues("Carlsen")).containsExactly("a", "b", "c")
    assertThat(getValues("Caruana")).containsExactly("a", null, "c")
  }

  private def getValues(key: String) = collection.getDocument(key, classOf[BaseDocument])
    .getAttribute("values")
    .asInstanceOf[List[String]]
    .asJava

  private def getInfo(key: String) = collection.getDocument(key, classOf[BaseDocument])
    .getAttribute("info")
    .asInstanceOf[Map[String, String]]
    .asJava

}
