package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ReadMode}
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class ArangoScan(ctx: PushDownCtx, options: ArangoDBConf) extends Scan with Batch {
  ExprUtils.verifyColumnNameOfCorruptRecord(ctx.requiredSchema, options.readOptions.columnNameOfCorruptRecord)

  override def readSchema(): StructType = ctx.requiredSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = options.readOptions.readMode match {
    case ReadMode.Query => Array(SingletonPartition)
    case ReadMode.Collection => planCollectionPartitions().asInstanceOf[Array[InputPartition]]
  }

  override def createReaderFactory(): PartitionReaderFactory = new ArangoPartitionReaderFactory(ctx, options)

  private def planCollectionPartitions() =
    ArangoClient.getCollectionShardIds(options)
      .zip(Stream.continually(options.driverOptions.endpoints).flatten)
      .map(it => new ArangoCollectionPartition(it._1, it._2))

}
