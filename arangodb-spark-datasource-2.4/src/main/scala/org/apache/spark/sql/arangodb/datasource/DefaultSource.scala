package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.datasource.reader.ArangoDataSourceReader
import org.apache.spark.sql.arangodb.util.ArangoUtils
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

  private def inferSchema(options: ArangoOptions): StructType = {
    if (inferredSchema == null) {
      inferredSchema = ArangoUtils.inferSchema(options)
    }
    inferredSchema
  }

  override def createReader(options: DataSourceOptions): DataSourceReader =
    createReader(inferSchema(ArangoOptions(options.asMap())), options)

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    new ArangoDataSourceReader(schema, ArangoOptions(options.asMap()))

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???

  override def shortName(): String = "arangodb"

}
