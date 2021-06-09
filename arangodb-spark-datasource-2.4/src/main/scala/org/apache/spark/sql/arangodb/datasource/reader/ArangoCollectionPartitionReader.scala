package org.apache.spark.sql.arangodb.datasource.reader

import com.arangodb.velocypack.VPackSlice
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions}
import org.apache.spark.sql.arangodb.datasource.mapping.ArangoParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader


class ArangoCollectionPartitionReader(
                                       inputPartition: ArangoCollectionPartition,
                                       ctx: PushDownCtx,
                                       opts: ArangoOptions)
  extends InputPartitionReader[InternalRow] {

  // override endpoints with partition endpoint
  private val options = opts.updated(ArangoOptions.ENDPOINTS, inputPartition.endpoint)
  private val parser = ArangoParser.of(options.readOptions.contentType, ctx.requiredSchema)
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
