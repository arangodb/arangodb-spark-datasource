package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ArangoBatchWriter(schema: StructType, options: ArangoOptions, mode: SaveMode) extends BatchWrite {
  private val client = ArangoClient(options)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new ArangoDataWriterFactory(schema, options)

  // TODO
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    println("ArangoBatchWriter::commit")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    mode match {
      case SaveMode.Append => throw new DataWriteAbortException(
        "Cannot abort with SaveMode.Append: the underlying data source may require manual cleanup.")
      case SaveMode.Overwrite => client.truncate()
      case SaveMode.ErrorIfExists => client.drop()
      case SaveMode.Ignore => // do nothing
    }
  }

}
