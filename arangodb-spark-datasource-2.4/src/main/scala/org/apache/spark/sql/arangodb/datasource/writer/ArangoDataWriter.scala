package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.ArangoDBMultipleException
import com.arangodb.model.OverwriteMode
import com.arangodb.velocypack.{VPackParser, VPackSlice}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.exceptions.{ArangoDBDataWriterException, DataWriteAbortException}
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGenerator, ArangoGeneratorProvider}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.io.ByteArrayOutputStream
import java.net.{ConnectException, UnknownHostException}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
import scala.util.Random

class ArangoDataWriter(schema: StructType, options: ArangoDBConf, partitionId: Int)
  extends DataWriter[InternalRow] with Logging {

  private var failures = 0
  private var exceptions: List[Exception] = List()
  private var requestCount = 0L
  private var endpointIdx = partitionId
  private val endpoints = Stream.continually(options.driverOptions.endpoints).flatten
  private val rnd = new Random()
  private var client: ArangoClient = createClient()
  private var batchCount: Int = _
  private var outVPack: ByteArrayOutputStream = _
  private var vpackGenerator: ArangoGenerator = _

  initBatch()

  override def write(record: InternalRow): Unit = {
    vpackGenerator.write(record)
    vpackGenerator.flush()
    batchCount += 1
    if (batchCount == options.writeOptions.batchSize || outVPack.size() > options.writeOptions.byteBatchSize) {
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

  private def canRetry: Boolean = ArangoDataWriter.canRetry(schema, options)

  private def initBatch(): Unit = {
    batchCount = 0
    outVPack = new ByteArrayOutputStream()
    vpackGenerator = ArangoGeneratorProvider().of(options.driverOptions.contentType, schema, outVPack, options)
    vpackGenerator.writeStartArray()
  }

  private def flushBatch(): Unit = {
    vpackGenerator.writeEndArray()
    vpackGenerator.close()
    vpackGenerator.flush()
    logDebug(s"flushBatch(), bufferSize: ${outVPack.size()}")
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
      exceptions = List()
    } catch {
      case e: Exception =>
        client.shutdown()
        failures += 1
        exceptions = e :: exceptions
        endpointIdx += 1
        if ((canRetry || isConnectionException(e)) && failures < options.writeOptions.maxAttempts) {
          val delay = computeDelay()
          logWarning(s"Got exception while saving documents, retrying in $delay ms:", e)
          Thread.sleep(delay)
          client = createClient()
          saveDocuments(payload)
        } else {
          throw new ArangoDBDataWriterException(exceptions.reverse.toArray)
        }
    }
  }

  private def computeDelay(): Int = {
    val min = options.writeOptions.minRetryDelay
    val max = options.writeOptions.maxRetryDelay
    val diff = max - min
    val delta = if (diff <= 0) 0 else rnd.nextInt(diff)
    min + delta
  }

  private def isConnectionException(e: Exception): Boolean = e.getCause match {
    case mEx: ArangoDBMultipleException =>
      mEx.getExceptions.asScala.forall {
        case _: ConnectException => true
        case _: UnknownHostException => true
        case _ => false
      }
    case _ => false
  }

}

object ArangoDataWriter {
  def canRetry(schema: StructType, options: ArangoDBConf): Boolean =
    schema.exists(p => p.name == "_key" && !p.nullable) && (options.writeOptions.overwriteMode match {
      case OverwriteMode.ignore => true
      case OverwriteMode.replace => true
      case OverwriteMode.update => options.writeOptions.keepNull
      case OverwriteMode.conflict => false
    })
}
