package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.entity.CollectionType
import com.arangodb.model.OverwriteMode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ContentType}
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, SaveMode}

class ArangoWriterBuilder(schema: StructType, options: ArangoDBConf)
  extends WriteBuilder with SupportsTruncate with Logging {

  private var mode: SaveMode = SaveMode.Append
  validateConfig()

  override def buildForBatch(): BatchWrite = {
    val client = ArangoClient(options)
    if (!client.collectionExists()) {
      client.createCollection()
    }
    client.shutdown()

    val updatedOptions = options.updated(ArangoDBConf.OVERWRITE_MODE, mode match {
      case SaveMode.Append => options.writeOptions.overwriteMode.getValue
      case _ => OverwriteMode.ignore.getValue
    })

    logSummary(updatedOptions)
    new ArangoBatchWriter(schema, updatedOptions, mode)
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

  private def validateConfig(): Unit = {
    if (options.driverOptions.contentType == ContentType.JSON && hasDecimalTypeFields) {
      throw new UnsupportedOperationException("Cannot write DecimalType when using contentType=json")
    }

    if (options.writeOptions.collectionType == CollectionType.EDGES &&
      !schema.exists(p => p.name == "_from" && p.dataType == StringType && !p.nullable)
    ) {
      throw new IllegalArgumentException("Writing edge collection requires non nullable string field named _from.")
    }

    if (options.writeOptions.collectionType == CollectionType.EDGES &&
      !schema.exists(p => p.name == "_to" && p.dataType == StringType && !p.nullable)
    ) {
      throw new IllegalArgumentException("Writing edge collection requires non nullable string field named _to.")
    }
  }

  private def hasDecimalTypeFields: Boolean =
    schema.existsRecursively {
      case _: DecimalType => true
      case _ => false
    }

  private def logSummary(updatedOptions: ArangoDBConf): Unit = {
    val canRetry = ArangoDataWriter.canRetry(schema, updatedOptions)

    logInfo(s"Using save mode: $mode")
    logInfo(s"Using write configuration: ${updatedOptions.writeOptions}")
    logInfo(s"Using mapping configuration: ${updatedOptions.mappingOptions}")
    logInfo(s"Can retry: $canRetry")

    if (!canRetry) {
      logWarning(
        """The provided configuration does not allow idempotent requests: write failures will not be retried and lead
          |to task failure. Speculative task executions could fail or write incorrect data."""
          .stripMargin.replaceAll("\n", "")
      )
    }
  }

}
