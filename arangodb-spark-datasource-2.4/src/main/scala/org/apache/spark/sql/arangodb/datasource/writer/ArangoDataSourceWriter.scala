package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ArangoDataSourceWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: ArangoOptions) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] =
    new ArangoDataWriterFactory(schema, options)

  // TODO
  override def commit(messages: Array[WriterCommitMessage]): Unit =
    println("ArangoDataSourceWriter::commit")

  // TODO
  override def abort(messages: Array[WriterCommitMessage]): Unit = ???


}
