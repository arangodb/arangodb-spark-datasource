package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoUtils}
import org.apache.spark.sql.arangodb.datasource.reader.ArangoDataSourceReader
import org.apache.spark.sql.arangodb.datasource.writer.ArangoDataSourceWriter
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

import java.util.Optional

class DefaultSource extends DataSourceV2 with DataSourceRegister
  with ReadSupport
  with WriteSupport {

  private var inferredSchema: StructType = _

  private def inferSchema(options: ArangoDBConf): StructType = {
    if (inferredSchema == null) {
      inferredSchema = ArangoUtils.inferSchema(options)
    }
    inferredSchema
  }

  private def extractOptions(options: DataSourceOptions): ArangoDBConf = {
    val opts: ArangoDBConf = ArangoDBConf(options.asMap())
    if (opts.driverOptions.acquireHostList) {
      val hosts = ArangoClient.acquireHostList(opts)
      opts.updated(ArangoDBConf.ENDPOINTS, hosts.mkString(","))
    } else {
      opts
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val opts = extractOptions(options)
    new ArangoDataSourceReader(inferSchema(opts), opts)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    new ArangoDataSourceReader(schema, extractOptions(options))

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] =
    Optional.of(new ArangoDataSourceWriter(writeUUID, schema, mode, extractOptions(options)))

  override def shortName(): String = "arangodb"

}
