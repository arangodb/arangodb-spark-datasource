package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, CollectionType}
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.JavaConverters._


class CreateCollectionTest extends BaseSparkTest {

  private val collectionName = "chessPlayersCreateCollection"
  private val collection: ArangoCollection = db.collection(collectionName)

  private val rows = Seq(
    Row("a/1", "b/1"),
    Row("a/2", "b/2"),
    Row("a/3", "b/3"),
    Row("a/4", "b/4"),
    Row("a/5", "b/5"),
    Row("a/6", "b/6")
  )

  private val df = spark.createDataFrame(rows.asJava, StructType(Array(
    StructField("_from", StringType, nullable = false),
    StructField("_to", StringType, nullable = false)
  ))).repartition(3)

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
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.NUMBER_OF_SHARDS -> "5",
        ArangoDBConf.COLLECTION_TYPE -> CollectionType.EDGE.name
      ))
      .save()

    if (isCluster) {
      assertThat(collection.getProperties.getNumberOfShards).isEqualTo(5)
    }
    assertThat(collection.getProperties.getType.getType).isEqualTo(com.arangodb.entity.CollectionType.EDGES.getType)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwrite(protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.CONFIRM_TRUNCATE -> "true",
        ArangoDBConf.NUMBER_OF_SHARDS -> "5",
        ArangoDBConf.COLLECTION_TYPE -> CollectionType.EDGE.name
      ))
      .save()

    if (isCluster) {
      assertThat(collection.getProperties.getNumberOfShards).isEqualTo(5)
    }
    assertThat(collection.getProperties.getType.getType).isEqualTo(com.arangodb.entity.CollectionType.EDGES.getType)
  }

}
