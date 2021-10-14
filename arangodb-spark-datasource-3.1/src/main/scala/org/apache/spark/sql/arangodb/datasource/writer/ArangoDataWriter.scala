package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.ArangoDBException
import com.arangodb.model.OverwriteMode
import com.arangodb.velocypack.{VPackParser, VPackSlice}
import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGenerator, ArangoGeneratorProvider}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.io.ByteArrayOutputStream
import scala.annotation.tailrec

class ArangoDataWriter(schema: StructType, options: ArangoOptions, partitionId: Int) extends DataWriter[InternalRow] {
  private var failures = 0
  private var endpointIdx = partitionId
  private val endpoints = Stream.continually(options.driverOptions.endpoints).flatten
  private var client: ArangoClient = createClient()
  private var batchCount: Int = _
  private var outVPack: ByteArrayOutputStream = _
  private var vpackGenerator: ArangoGenerator = _

  initBatch()

  override def write(record: InternalRow): Unit = {
    vpackGenerator.write(record)
    batchCount += 1
    if (batchCount == options.writeOptions.batchSize) {
      flushBatch()
      initBatch()
    }
  }

  override def commit(): WriterCommitMessage = {
    flushBatch()
    null
  }

  /**
   * Data cleanup will happen in [[ArangoBatchWriter.abort()]]
   */
  override def abort(): Unit = {
    if (!canRetry)
      throw new DataWriteAbortException(
        """
          |Task cannot be aborted: the operation will not be retried. Consider using one of the following:
          |- OverwriteMode.ignore
          |- OverwriteMode.replace
          |- OverwriteMode.update
          |to make batch writes idempotent, so that they can be retried."""
          .stripMargin.replaceAll("\n", " "))
  }

  override def close(): Unit = {
    client.shutdown()
  }

  private def createClient() = ArangoClient(options.updated(ArangoOptions.ENDPOINTS, endpoints(endpointIdx)))

  private def canRetry: Boolean =
    if (options.writeOptions.overwriteMode.isEmpty) false
    else options.writeOptions.overwriteMode.get match {
      case OverwriteMode.ignore => true
      case OverwriteMode.replace => true
      case OverwriteMode.update => true
      case OverwriteMode.conflict => false
    }

  private def initBatch(): Unit = {
    batchCount = 0
    outVPack = new ByteArrayOutputStream()
    vpackGenerator = ArangoGeneratorProvider().of(options.writeOptions.contentType, schema, outVPack)
    vpackGenerator.writeStartArray()
  }

  private def flushBatch(): Unit = {
    vpackGenerator.writeEndArray()
    vpackGenerator.close()
    vpackGenerator.flush()
    val payload = options.writeOptions.contentType match {
      case ContentType.VPack => new VPackSlice(outVPack.toByteArray)
      case ContentType.Json => new VPackParser.Builder().build().fromJson(new String(outVPack.toByteArray), true)
    }
    saveDocuments(payload)
  }

  @tailrec private def saveDocuments(payload: VPackSlice): Unit = {
    try {
      client.saveDocuments(payload)
      failures = 0
    } catch {
      case e: ArangoDBException =>
        // TODO: log warn e
        client.shutdown()
        failures += 1
        endpointIdx += 1
        if (canRetry && failures < options.driverOptions.endpoints.size) {
          client = createClient()
          saveDocuments(payload)
        } else throw e
    }
  }

}
