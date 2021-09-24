package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ArangoDataSourceWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: ArangoOptions) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {

    if (mode == SaveMode.Overwrite && !options.writeOptions.confirmTruncate) {
      throw new AnalysisException(
        """You are attempting to use overwrite mode which will truncate
          |this collection prior to inserting data. If you just want
          |to change data already in the collection use the "Append" mode.
          |To actually truncate please set "confirm.truncate" option to "true".""".stripMargin
      )
    }

    val client = ArangoClient(options)
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
    }

    new ArangoDataWriterFactory(schema, options)
  }

  // TODO
  override def commit(messages: Array[WriterCommitMessage]): Unit =
    println("ArangoDataSourceWriter::commit")

  // TODO
  override def abort(messages: Array[WriterCommitMessage]): Unit = ???


}
