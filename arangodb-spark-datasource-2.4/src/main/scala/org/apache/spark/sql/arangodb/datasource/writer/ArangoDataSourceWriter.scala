package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ArangoDataSourceWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: ArangoOptions) extends DataSourceWriter {
  private val client = ArangoClient(options)
  private var createdCollection = false

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    if (mode == SaveMode.Overwrite && !options.writeOptions.confirmTruncate) {
      throw new AnalysisException(
        "You are attempting to use overwrite mode which will truncate this collection prior to inserting data. If " +
          "you just want to change data already in the collection set save mode 'append' and " +
          "'overwrite.mode=(replace|update)'. To actually truncate set 'confirm.truncate=true'.")
    }

    if (client.collectionExists()) {
      mode match {
        case SaveMode.Append => // do nothing
        case SaveMode.Overwrite => ArangoClient(options).truncate()
        case SaveMode.ErrorIfExists => throw new AnalysisException(
          s"Collection '${options.writeOptions.collection}' already exists. SaveMode: ErrorIfExists.")
        case SaveMode.Ignore => return new NoOpDataWriterFactory
      }
    } else {
      client.createCollection()
      createdCollection = true
    }

    new ArangoDataWriterFactory(schema, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    client.shutdown()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    mode match {
      case SaveMode.Append => throw new DataWriteAbortException(
        "Cannot abort with SaveMode.Append: the underlying data source may require manual cleanup.")
      case SaveMode.Overwrite => client.truncate()
      case SaveMode.ErrorIfExists => client.drop()
      case SaveMode.Ignore => if (createdCollection) client.drop()
    }
    client.shutdown()
  }

}
