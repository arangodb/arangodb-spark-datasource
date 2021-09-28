package org.apache.spark.sql.arangodb.datasource.reader

import com.arangodb.velocypack.VPackSlice
import org.apache.spark.sql.arangodb.commons.mapping.ArangoParserProvider
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader


class ArangoCollectionPartitionReader(inputPartition: ArangoCollectionPartition, ctx: PushDownCtx, opts: ArangoOptions)
  extends PartitionReader[InternalRow] {

  // override endpoints with partition endpoint
  private val options = opts.updated(ArangoOptions.ENDPOINTS, inputPartition.endpoint)
  private val parser = ArangoParserProvider().of(options.readOptions.contentType, ctx.requiredSchema)
  private lazy val client = ArangoClient(options)
  private lazy val iterator = client.readCollectionPartition(inputPartition.shardId, ctx)

  private var current: VPackSlice = _

  override def next: Boolean =
    if (iterator.hasNext) {
      current = iterator.next()
      true
    } else {
      false
    }

  override def get: InternalRow = parser.parse(current.toByteArray).head

  override def close(): Unit = {
    iterator.close()
    client.shutdown()
  }

}
