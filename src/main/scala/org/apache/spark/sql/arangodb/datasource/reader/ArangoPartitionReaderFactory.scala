package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.datasource.ArangoOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class ArangoPartitionReaderFactory(schema: StructType, options: ArangoOptions) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = partition match {
    case p: ArangoCollectionPartition => new ArangoCollectionPartitionReader(p, schema, options)
    case SingletonPartition => new ArangoQueryReader(schema, options)
  }
}