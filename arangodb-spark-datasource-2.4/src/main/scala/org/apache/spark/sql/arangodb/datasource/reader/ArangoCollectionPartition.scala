package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType


/**
 * Partition corresponding to an Arango collection shard
 *
 * @param shardId  collection shard id
 * @param endpoint db endpoint to use to query the partition
 */
class ArangoCollectionPartition(
                                 val shardId: String,
                                 val endpoint: String,
                                 val ctx: PushDownCtx,
                                 val options: ArangoOptions
                               ) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new ArangoCollectionPartitionReader(this, ctx, options)
}

/**
 * Custom user queries will not be partitioned (eg. AQL traversals)
 */
class SingletonPartition(
                          val schema: StructType,
                          val options: ArangoOptions
                        ) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new ArangoQueryReader(schema, options)
}
