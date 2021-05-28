package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.{ArangoOptions, FilterSupport, PushableFilter, PushdownUtils}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ArangoScanBuilder(options: ArangoOptions, tableSchema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var requiredSchema: StructType = _

  // fully or partially applied filters
  private var appliedFilters: Array[Filter] = Array()

  // partially or not applied filters
  private var toEvaluateFilters: Array[Filter] = Array()

  override def build(): Scan = new ArangoScan(requiredSchema, appliedFilters, options)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    appliedFilters = filters.filter(PushableFilter(_, tableSchema).support != FilterSupport.NONE)
    toEvaluateFilters = filters.filter(PushableFilter(_, tableSchema).support != FilterSupport.FULL)
    toEvaluateFilters
  }

  override def pushedFilters(): Array[Filter] = appliedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
