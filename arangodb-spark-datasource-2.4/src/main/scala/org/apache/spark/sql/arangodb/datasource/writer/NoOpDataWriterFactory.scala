package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}

class NoOpDataWriterFactory() extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] =
    new NoOpDataWriter
}
