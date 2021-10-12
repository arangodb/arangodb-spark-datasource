package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

class NoOpDataWriter() extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = {
    // do nothing
  }

  override def commit(): WriterCommitMessage = null

  override def abort(): Unit = {
    // do nothing
  }

}
