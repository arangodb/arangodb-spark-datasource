//package org.apache.spark.sql.arangodb.datasource.writer
//
//import org.apache.spark.sql.arangodb.datasource.ArangoOptions
//import org.apache.spark.sql.connector.write.{BatchWrite, WriteBuilder}
//import org.apache.spark.sql.types.StructType
//
//class ArangoWriterBuilder(schema: StructType, options: ArangoOptions) extends WriteBuilder {
//  override def buildForBatch(): BatchWrite = new ArangoBatchWriter(schema, options)
//}
