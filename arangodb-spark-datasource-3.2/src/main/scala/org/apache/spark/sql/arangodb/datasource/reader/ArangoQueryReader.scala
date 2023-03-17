package org.apache.spark.sql.arangodb.datasource.reader

import com.arangodb.entity.CursorWarning
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.mapping.ArangoParserProvider
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.JavaConverters.iterableAsScalaIterableConverter


class ArangoQueryReader(schema: StructType, options: ArangoDBConf) extends PartitionReader[InternalRow] with Logging {

  private val actualSchema = StructType(schema.filterNot(_.name == options.readOptions.columnNameOfCorruptRecord))
  private val parser = ArangoParserProvider().of(options.driverOptions.contentType, actualSchema, options)
  private val safeParser = new FailureSafeParser[Array[Byte]](
    parser.parse,
    options.readOptions.parseMode,
    schema,
    options.readOptions.columnNameOfCorruptRecord)
  private val client = ArangoClient(options)
  private val iterator = client.readQuery()

  var rowIterator: Iterator[InternalRow] = _

  // warnings of non stream AQL cursors are all returned along with the first batch
  if (!options.readOptions.stream) logWarns()

  @tailrec
  final override def next: Boolean =
    if (iterator.hasNext) {
      val current = iterator.next()
      rowIterator = safeParser.parse(current.get)
      if (rowIterator.hasNext) {
        true
      } else {
        next
      }
    } else {
      // FIXME: https://arangodb.atlassian.net/browse/BTS-671
      // stream AQL cursors' warnings are only returned along with the final batch
      if (options.readOptions.stream) logWarns()
      false
    }

  override def get: InternalRow = rowIterator.next()

  override def close(): Unit = {
    iterator.close()
    client.shutdown()
  }

  private def logWarns(): Unit = Option(iterator.getWarnings).foreach(_.asScala.foreach((w: CursorWarning) =>
    logWarning(s"Got AQL warning: [${w.getCode}] ${w.getMessage}")
  ))

}


