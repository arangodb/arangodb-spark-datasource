package org.apache.spark.sql.arangodb.datasource.reader

import com.arangodb.velocypack.VPackSlice
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.mapping.ArangoParserProvider
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.nio.charset.StandardCharsets


class ArangoCollectionPartitionReader(inputPartition: ArangoCollectionPartition, ctx: PushDownCtx, opts: ArangoOptions)
  extends PartitionReader[InternalRow] with Logging {

  // override endpoints with partition endpoint
  private val options = opts.updated(ArangoOptions.ENDPOINTS, inputPartition.endpoint)
  private val parser = ArangoParserProvider().of(options.readOptions.contentType, ctx.requiredSchema)
  private val client = ArangoClient(options)
  private val iterator = client.readCollectionPartition(inputPartition.shardId, ctx)

  private var current: VPackSlice = _

  // warnings of non stream AQL cursors are all returned along with the first batch
  if (!options.readOptions.stream) logWarns()

  override def next: Boolean =
    if (iterator.hasNext) {
      current = iterator.next()
      true
    } else {
      // FIXME: https://arangodb.atlassian.net/browse/BTS-671
      // stream AQL cursors' warnings are only returned along with the final batch
      if (options.readOptions.stream) logWarns()
      false
    }

  override def get: InternalRow = options.readOptions.contentType match {
    case ContentType.VPack => parser.parse(current.toByteArray).head
    case ContentType.Json => parser.parse(current.toString.getBytes(StandardCharsets.UTF_8)).head
  }

  override def close(): Unit = {
    iterator.close()
    client.shutdown()
  }

  private def logWarns(): Unit = Option(iterator.getWarnings).foreach(_.forEach(w => {
    logWarning(s"Got AQL warning: [${w.getCode}] ${w.getMessage}")
  }))

}
