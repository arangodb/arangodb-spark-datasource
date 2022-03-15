package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ContentType}
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.{AnalysisException, SaveMode}

class ArangoWriterBuilder(schema: StructType, options: ArangoDBConf) extends WriteBuilder with SupportsTruncate {

  private var mode: SaveMode = SaveMode.Append

  if (options.driverOptions.contentType == ContentType.JSON && hasDecimalTypeFields)
    throw new UnsupportedOperationException("Cannot write DecimalType when using contentType=json")

  override def buildForBatch(): BatchWrite = {
    val client = ArangoClient(options)
    if (!client.collectionExists()) {
      client.createCollection()
    }
    client.shutdown()
    new ArangoBatchWriter(schema, options, mode)
  }

  override def truncate(): WriteBuilder = {
    mode = SaveMode.Overwrite
    if (options.writeOptions.confirmTruncate) {
      val client = ArangoClient(options)
      if (client.collectionExists()) {
        client.truncate()
      } else {
        client.createCollection()
      }
      client.shutdown()
      this
    } else {
      throw new AnalysisException(
        "You are attempting to use overwrite mode which will truncate this collection prior to inserting data. If " +
          "you just want to change data already in the collection set save mode 'append' and " +
          s"'overwrite.mode=(replace|update)'. To actually truncate set '${ArangoDBConf.CONFIRM_TRUNCATE}=true'.")
    }
  }

  private def hasDecimalTypeFields: Boolean =
    schema.existsRecursively {
      case _: DecimalType => true
      case _ => false
    }

}
