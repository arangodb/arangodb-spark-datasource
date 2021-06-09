package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.arangodb.commons.filter.{FilterSupport, PushableFilter}
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ArangoScanBuilder(options: ArangoOptions, tableSchema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var requiredSchema: StructType = _

  // fully or partially applied filters
  private var appliedPushableFilters: Array[PushableFilter] = Array()
  private var appliedSparkFilters: Array[Filter] = Array()

  override def build(): Scan = new ArangoScan(new PushDownCtx(requiredSchema, appliedPushableFilters), options)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val filtersBySupport = filters
      .map(f => (f, PushableFilter(f, tableSchema)))
      .groupBy(_._2.support())

    val appliedFilters = filtersBySupport
      .filter(_._1 != FilterSupport.NONE)
      .values.flatten

    appliedPushableFilters = appliedFilters.map(_._2).toArray
    appliedSparkFilters = appliedFilters.map(_._1).toArray

    // partially or not applied filters
    val toEvaluateFilters = filtersBySupport
      .filter(_._1 != FilterSupport.FULL)
      .values.flatten
      .map(_._1).toArray

    println("\n--- APPLIED FILTERS:")
    println(appliedSparkFilters.mkString("\n"))
    println("\n--- TO EVALUATE FILTERS:")
    println(toEvaluateFilters.mkString("\n"))

    toEvaluateFilters
  }

  override def pushedFilters(): Array[Filter] = appliedSparkFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
