package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.connector.read.InputPartition

/**
 * Partition corresponding to an Arango collection shard
 * @param shardId collection shard id
 * @param endpoint db endpoint to use to query the partition
 */
class ArangoCollectionPartition(val shardId: String, val endpoint: String) extends InputPartition

/**
 * Custom user queries will not be partitioned (eg. AQL traversals)
 */
object SingletonPartition extends InputPartition
