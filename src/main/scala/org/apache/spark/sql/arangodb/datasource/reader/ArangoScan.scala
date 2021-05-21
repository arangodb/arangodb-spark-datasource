package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.datasource.{ArangoOptions, ReadMode}
import org.apache.spark.sql.arangodb.util.ArangoClient
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ArangoScan(schema: StructType, filters: Array[Filter], options: ArangoOptions) extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = options.readOptions.readMode match {
    case ReadMode.Query => Array(SingletonPartition)
    case ReadMode.Collection => planCollectionPartitions().asInstanceOf[Array[InputPartition]]
  }

  override def createReaderFactory(): PartitionReaderFactory = new ArangoPartitionReaderFactory(schema, filters, options)

  private def planCollectionPartitions() =
    ArangoClient.getCollectionShardIds(options)
      .zip(Stream.continually(options.driverOptions.endpoints).flatten)
      .map(it => new ArangoCollectionPartition(it._1, it._2))

}