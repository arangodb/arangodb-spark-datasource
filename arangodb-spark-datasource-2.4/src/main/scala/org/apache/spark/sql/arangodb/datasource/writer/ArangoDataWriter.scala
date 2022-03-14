package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.model.OverwriteMode
import com.arangodb.velocypack.{VPackParser, VPackSlice}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGenerator, ArangoGeneratorProvider}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.io.ByteArrayOutputStream
import scala.annotation.tailrec

class ArangoDataWriter(schema: StructType, options: ArangoDBConf, partitionId: Int)
  extends DataWriter[InternalRow] with Logging {

  private var failures = 0
  private var requestCount = 0L
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
    client.shutdown()
    null // scalastyle:ignore null
  }

  /**
   * Data cleanup will happen in [[ArangoDataSourceWriter.abort()]]
   */
  override def abort(): Unit = {
    client.shutdown()
    if (!canRetry) {
      throw new DataWriteAbortException(
        "Task cannot be retried. To make batch writes idempotent, so that they can be retried, consider using " +
          "'keep.null=true' (default) and 'overwrite.mode=(ignore|replace|update)'.")
    }
  }

  private def createClient() = ArangoClient(options.updated(ArangoDBConf.ENDPOINTS, endpoints(endpointIdx)))

  private def canRetry: Boolean =
    options.writeOptions.overwriteMode match {
      case OverwriteMode.ignore => true
      case OverwriteMode.replace => true
      case OverwriteMode.update => options.writeOptions.keepNull
      case OverwriteMode.conflict => false
    }

  private def initBatch(): Unit = {
    batchCount = 0
    outVPack = new ByteArrayOutputStream()
    vpackGenerator = ArangoGeneratorProvider().of(options.driverOptions.contentType, schema, outVPack)
    vpackGenerator.writeStartArray()
  }

  private def flushBatch(): Unit = {
    vpackGenerator.writeEndArray()
    vpackGenerator.close()
    vpackGenerator.flush()
    val payload = options.driverOptions.contentType match {
      case ContentType.VPACK => new VPackSlice(outVPack.toByteArray)
      case ContentType.JSON => new VPackParser.Builder().build().fromJson(new String(outVPack.toByteArray), true)
    }
    saveDocuments(payload)
  }

  @tailrec private def saveDocuments(payload: VPackSlice): Unit = {
    try {
      requestCount += 1
      logDebug(s"Sending request #$requestCount for partition $partitionId")
      client.saveDocuments(payload)
      logDebug(s"Received response #$requestCount for partition $partitionId")
      failures = 0
    } catch {
      case e: Exception =>
        client.shutdown()
        failures += 1
        endpointIdx += 1
        if (canRetry && failures < options.driverOptions.endpoints.length * 3) {
          logWarning("Got exception while saving documents: ", e)
          client = createClient()
          saveDocuments(payload)
        } else {
          throw e
        }
    }
  }

}
