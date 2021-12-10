package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class ArangoPartitionReaderFactory(ctx: PushDownCtx, options: ArangoDBConf) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = partition match {
    case p: ArangoCollectionPartition => new ArangoCollectionPartitionReader(p, ctx, options)
    case SingletonPartition => new ArangoQueryReader(ctx.requiredSchema, options)
  }
}