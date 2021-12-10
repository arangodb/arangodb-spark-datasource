package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class ArangoDataWriterFactory(schema: StructType, options: ArangoDBConf) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new ArangoDataWriter(schema, options, partitionId)
  }
}
