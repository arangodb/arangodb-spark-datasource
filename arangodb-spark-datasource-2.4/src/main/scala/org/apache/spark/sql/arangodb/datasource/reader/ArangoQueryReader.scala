package org.apache.spark.sql.arangodb.datasource.reader

import com.arangodb.velocypack.{VPackParser, VPackSlice}
import org.apache.spark.sql.arangodb.commons.mapping.ArangoParserProvider
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types._


class ArangoQueryReader(schema: StructType, options: ArangoOptions) extends InputPartitionReader[InternalRow] {

  private val parser = ArangoParserProvider().of(options.readOptions.contentType, schema)
  private lazy val client = ArangoClient(options)
  private lazy val iterator = client.readQuery()

  private var current: VPackSlice = _

  override def next: Boolean =
    if (iterator.hasNext) {
      current = iterator.next()
      true
    } else {
      false
    }

  override def get: InternalRow = options.readOptions.contentType match {
    case ContentType.VPack => parser.parse(current.toByteArray).head
    case ContentType.Json => parser.parse(new VPackParser.Builder().build().toJson(current).getBytes).head
  }

  override def close(): Unit = {
    iterator.close()
    client.shutdown()
  }

}


