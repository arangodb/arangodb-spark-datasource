//package org.apache.spark.sql.arangodb.datasource.writer
//
//import org.apache.spark.sql.arangodb.datasource.ArangoOptions
//import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
//import org.apache.spark.sql.types.StructType
//
//class ArangoBatchWriter(schema: StructType, options: ArangoOptions) extends BatchWrite {
//  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
//    new ArangoDataWriterFactory(schema, options)
//
//  // TODO
//  override def commit(messages: Array[WriterCommitMessage]): Unit = {
//    println("ArangoBatchWriter::commit")
//  }
//
//  // TODO
//  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
//}
