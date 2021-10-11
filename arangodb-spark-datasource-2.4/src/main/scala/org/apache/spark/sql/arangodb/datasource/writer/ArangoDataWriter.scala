package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.model.OverwriteMode
import com.arangodb.velocypack.{VPackParser, VPackSlice}
import org.apache.spark.sql.arangodb.commons.exceptions.DataWriteAbortException
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGenerator, ArangoGeneratorProvider}
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, ContentType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.io.ByteArrayOutputStream

class ArangoDataWriter(schema: StructType, options: ArangoOptions) extends DataWriter[InternalRow] {
  private lazy val client = ArangoClient(options)
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
    null
  }

  /**
   * Data cleanup will happen in [[ArangoDataSourceWriter.abort()]]
   */
  override def abort(): Unit = {
    client.shutdown()
    options.writeOptions.overwriteMode.foreach {
      case OverwriteMode.ignore => // do nothing, task can be retried
      case OverwriteMode.replace => // do nothing, task can be retried
      case OverwriteMode.update => // throw if keepNull is false or not set (db default is false)
        if (options.writeOptions.keepNull.isEmpty || !options.writeOptions.keepNull.get) {
          throw new DataWriteAbortException(
            """
              |Cannot abort with OverwriteMode.update and keepNull=false: the operation will not be retried. Consider using
              |OverwriteMode.ignore, OverwriteMode.replace or OverwriteMode.update and keepNull=true to make batch writes
              |idempotent, so that they can be retried."""
              .stripMargin.replaceAll("\n", " "))
        }
      case OverwriteMode.conflict => throw new DataWriteAbortException(
        """
          |Cannot abort with OverwriteMode.conflict: the operation will not be retried. Consider using
          |OverwriteMode.ignore, OverwriteMode.replace or OverwriteMode.update and keepNull=true to make batch writes
          |idempotent, so that they can be retried."""
          .stripMargin.replaceAll("\n", " "))
    }
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
    client.saveDocuments(payload)
  }

}
