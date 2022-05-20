package org.apache.spark.sql.arangodb.datasource.write

import com.arangodb.ArangoCollection
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.datasource.BaseSparkTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class WriteWithNullKey extends BaseSparkTest {
  private val collectionName = "writeWithNullKey"
  private val collection: ArangoCollection = db.collection(collectionName)

  import spark.implicits._

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def writeWithNullKey(protocol: String, contentType: String): Unit = {
    Seq(
      ("Carlsen", "Magnus"),
      (null, "Fabiano")
    )
      .toDF("_key", "name")
      .write
      .format(BaseSparkTest.arangoDatasource)
      .mode(SaveMode.Overwrite)
      .options(options + (
        ArangoDBConf.COLLECTION -> collectionName,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.CONFIRM_TRUNCATE -> "true"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(2L)
  }

}
