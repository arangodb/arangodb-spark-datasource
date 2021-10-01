package org.apache.spark.sql.arangodb.datasource.writer

import com.arangodb.velocypack.{VPackParser, VPackSlice}
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

  // TODO
  override def abort(): Unit = {
    client.shutdown()
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
