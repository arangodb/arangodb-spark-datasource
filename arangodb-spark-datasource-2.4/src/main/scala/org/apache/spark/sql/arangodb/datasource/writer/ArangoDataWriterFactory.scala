package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.arangodb.datasource.ArangoOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class ArangoDataWriterFactory(schema: StructType, options: ArangoOptions) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    val endpoints = Stream.continually(options.driverOptions.endpoints).flatten
    new ArangoDataWriter(schema, options.updated(ArangoOptions.ENDPOINTS, endpoints(partitionId)))
  }
}
