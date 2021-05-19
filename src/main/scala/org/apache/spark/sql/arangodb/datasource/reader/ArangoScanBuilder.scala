package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.datasource.ArangoOptions
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ArangoScanBuilder(options: ArangoOptions) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var schema: StructType = _

  override def build(): Scan = new ArangoScan(schema, options)

  // TODO
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    println("=== pushFilters ===")
    filters.foreach(println)
    filters
  }

  // TODO
  override def pushedFilters(): Array[Filter] = {
    Array.empty
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    schema = requiredSchema
  }
}
