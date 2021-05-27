package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ArangoPartitionReaderFactory(schema: StructType, filters: Array[Filter], options: ArangoOptions) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = partition match {
    case p: ArangoCollectionPartition => new ArangoCollectionPartitionReader(p, schema, filters, options)
    case SingletonPartition => new ArangoQueryReader(schema, options)
  }
}