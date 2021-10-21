package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
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
  ).toDF("surname", "name")
    .repartition(6)

  @BeforeEach
  def beforeEach(): Unit = {
    if (collection.exists()) {
      collection.drop()
    }
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def save(protocol: String, contentType: String): Unit = {
    df.write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoOptions.ENDPOINTS -> (BaseSparkTest.endpoints + ",wrongHost:8529"),
        ArangoOptions.COLLECTION -> collectionName,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType,
        ArangoOptions.CONFIRM_TRUNCATE -> "true",
        ArangoOptions.OVERWRITE_MODE -> "replace"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }

}
