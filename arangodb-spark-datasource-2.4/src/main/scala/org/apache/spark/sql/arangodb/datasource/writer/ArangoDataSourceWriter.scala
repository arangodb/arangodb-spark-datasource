package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ArangoDataSourceWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: ArangoOptions) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    if (mode == SaveMode.Overwrite) {
      if (options.writeOptions.confirmTruncate) {
        ArangoClient(options).truncate()
      } else {
        throw new IllegalArgumentException(
          """You are attempting to use overwrite mode which will truncate
            |this collection prior to inserting data. If you just want
            |to change data already in the collection use the "Append" mode.
            |To actually truncate please set "confirm.truncate" option to "true".""".stripMargin)
      }
    }

    new ArangoDataWriterFactory(schema, options)
  }

  // TODO
  override def commit(messages: Array[WriterCommitMessage]): Unit =
    println("ArangoDataSourceWriter::commit")

  // TODO
  override def abort(messages: Array[WriterCommitMessage]): Unit = ???


}
