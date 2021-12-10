package com.arangodb.spark

import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf}
import org.apache.spark.sql.arangodb.datasource.ArangoTable
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DefaultSource extends TableProvider with DataSourceRegister {

  private var table: ArangoTable = _

  private def extractOptions(options: util.Map[String, String]): ArangoDBConf = {
    val opts: ArangoDBConf = ArangoDBConf(options)
    if (opts.driverOptions.acquireHostList) {
      val hosts = ArangoClient.acquireHostList(opts)
      opts.updated(ArangoDBConf.ENDPOINTS, hosts.mkString(","))
    } else {
      opts
    }
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = getTable(options).schema()

  private def getTable(options: CaseInsensitiveStringMap): Table = getTable(null, null, options.asCaseSensitiveMap())

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    if (table == null) {
      table = new ArangoTable(schema, extractOptions(properties))
    }
    table
  }

  override def supportsExternalMetadata(): Boolean = true

  override def shortName(): String = "arangodb"

}
