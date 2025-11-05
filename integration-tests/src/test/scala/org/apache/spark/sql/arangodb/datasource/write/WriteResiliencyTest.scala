package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import com.arangodb.model.OverwriteMode
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Disabled}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


class WriteResiliencyTest extends BaseSparkTest {

  private val collectionName = "chessPlayersResiliency"
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
  ).toDF("_key", "name")
    .repartition(6)

  @BeforeEach
  def beforeEach(): Unit = {
    if (collection.exists()) {
      collection.truncate()
    } else {
      collection.create()
    }
  }

  @Disabled("manual test only")
  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def retryOnTimeout(protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.TIMEOUT -> "1",
        ArangoDBConf.ENDPOINTS -> BaseSparkTest.endpoints,
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.CONFIRM_TRUNCATE -> "true",
        ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def retryOnWrongHost(protocol: String, contentType: String): Unit = {
    retryOnBadHost(BaseSparkTest.endpoints + ",127.0.0.1:111", protocol, contentType)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def retryOnUnknownHost(protocol: String, contentType: String): Unit = {
    retryOnBadHost(BaseSparkTest.endpoints + ",wrongHost:8529", protocol, contentType)
  }

  private def retryOnBadHost(endpoints: String, protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Append)
      .options(options + (
        ArangoDBConf.ENDPOINTS -> endpoints,
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.CONFIRM_TRUNCATE -> "true",
        ArangoDBConf.OVERWRITE_MODE -> OverwriteMode.replace.getValue,
        ArangoDBConf.MAX_ATTEMPTS -> "4",
        ArangoDBConf.MIN_RETRY_DELAY -> "20",
        ArangoDBConf.MAX_RETRY_DELAY -> "40"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

}
