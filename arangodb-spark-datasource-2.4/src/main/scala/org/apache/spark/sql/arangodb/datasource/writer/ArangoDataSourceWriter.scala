package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.entity.CollectionType
import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}

class ArangoDataSourceWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: ArangoDBConf) extends DataSourceWriter {
  private var createdCollection = false
  validateConfig()

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    if (mode == SaveMode.Overwrite && !options.writeOptions.confirmTruncate) {
      throw new AnalysisException(
        "You are attempting to use overwrite mode which will truncate this collection prior to inserting data. If " +
          "you just want to change data already in the collection set save mode 'append' and " +
          s"'overwrite.mode=(replace|update)'. To actually truncate set '${ArangoDBConf.CONFIRM_TRUNCATE}=true'.")
    }

    val client = ArangoClient(options)
    if (client.collectionExists()) {
      mode match {
        case SaveMode.Append => // do nothing
        case SaveMode.Overwrite => ArangoClient(options).truncate()
        case SaveMode.ErrorIfExists => throw new AnalysisException(
          s"Collection '${options.writeOptions.collection}' already exists. SaveMode: ErrorIfExists.")
        case SaveMode.Ignore => return new NoOpDataWriterFactory // scalastyle:ignore return
      }
    } else {
      client.createCollection()
      createdCollection = true
    }

    client.shutdown()
    new ArangoDataWriterFactory(schema, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // nothing to do here
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val client = ArangoClient(options)
    mode match {
      case SaveMode.Append => throw new DataWriteAbortException(
        "Cannot abort with SaveMode.Append: the underlying data source may require manual cleanup.")
      case SaveMode.Overwrite => client.truncate()
      case SaveMode.ErrorIfExists => client.drop()
      case SaveMode.Ignore => if (createdCollection) client.drop()
    }
    client.shutdown()
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

}
