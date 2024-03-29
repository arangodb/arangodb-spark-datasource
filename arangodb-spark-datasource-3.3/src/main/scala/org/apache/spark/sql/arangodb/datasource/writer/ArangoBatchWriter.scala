package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf}
import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ArangoBatchWriter(schema: StructType, options: ArangoDBConf, mode: SaveMode) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new ArangoDataWriterFactory(schema, options)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // nothing to do here
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val client = ArangoClient(options)
    mode match {
      case SaveMode.Append => throw new DataWriteAbortException(
        "Cannot abort with SaveMode.Append: the underlying data source may require manual cleanup.")
      case SaveMode.Overwrite => client.truncate()
      case SaveMode.ErrorIfExists => ???
      case SaveMode.Ignore => ???
    }
    client.shutdown()
  }

}
