package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.StructType

class ArangoWriterBuilder(schema: StructType, options: ArangoOptions) extends WriteBuilder with SupportsTruncate {

  override def buildForBatch(): BatchWrite = new ArangoBatchWriter(schema, options)

  override def truncate(): WriteBuilder =
    if (options.writeOptions.confirmTruncate) {
      ArangoClient(options).truncate()
      this
    } else {
      throw new IllegalArgumentException(
        """You are attempting to use overwrite mode which will truncate
          |this collection prior to inserting data. If you just want
          |to change data already in the collection use the "Append" mode.
          |To actually truncate please set "confirm.truncate" option to "true".""".stripMargin)
    }

}
