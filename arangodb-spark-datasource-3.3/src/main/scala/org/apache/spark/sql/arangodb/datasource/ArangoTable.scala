package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, ArangoUtils}
import org.apache.spark.sql.arangodb.datasource.reader.ArangoScanBuilder
import org.apache.spark.sql.arangodb.datasource.writer.ArangoWriterBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.setAsJavaSetConverter

class ArangoTable(private var schemaOpt: Option[StructType], options: ArangoDBConf) extends Table with SupportsRead with SupportsWrite {
  private lazy val tableSchema = schemaOpt.getOrElse(ArangoUtils.inferSchema(options))

  override def name(): String = this.getClass.toString

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    //    TableCapability.STREAMING_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.TRUNCATE
    //    TableCapability.OVERWRITE_BY_FILTER,
    //    TableCapability.OVERWRITE_DYNAMIC,
  ).asJava

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder =
    new ArangoScanBuilder(options.updated(ArangoDBConf(scanOptions)), schema())

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new ArangoWriterBuilder(info.schema(), options.updated(ArangoDBConf(info.options())))
}
