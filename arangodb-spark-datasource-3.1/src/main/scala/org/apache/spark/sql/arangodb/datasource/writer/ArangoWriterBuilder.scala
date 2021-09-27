package org.apache.spark.sql.arangodb.datasource.writer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.StructType

class ArangoWriterBuilder(schema: StructType, options: ArangoOptions) extends WriteBuilder with SupportsTruncate {

  override def buildForBatch(): BatchWrite = {
    val client = ArangoClient(options)
    if (!client.collectionExists())
      client.createCollection()
    new ArangoBatchWriter(schema, options)
  }

  override def truncate(): WriteBuilder = {
    if (options.writeOptions.confirmTruncate) {
      val client = ArangoClient(options)
      if (client.collectionExists())
        client.truncate()
      else
        client.createCollection()
      this
    } else {
      throw new AnalysisException(
        """You are attempting to use overwrite mode which will truncate
          |this collection prior to inserting data. If you just want
          |to change data already in the collection use the "Append" mode.
          |To actually truncate please set "confirm.truncate" option to "true".""".stripMargin)
    }
  }

}
