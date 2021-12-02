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

    val fullSupp = filtersBySupport.getOrElse(FilterSupport.FULL, Array())
    val partialSupp = filtersBySupport.getOrElse(FilterSupport.PARTIAL, Array())
    val noneSupp = filtersBySupport.getOrElse(FilterSupport.NONE, Array())

    val appliedFilters = fullSupp ++ partialSupp
    appliedPushableFilters = appliedFilters.map(_._2)
    appliedSparkFilters = appliedFilters.map(_._1)

    if (fullSupp.nonEmpty)
      logInfo(s"Fully supported filters (applied in AQL):\n${fullSupp.map(_._1).mkString("\n")}")
    if (partialSupp.nonEmpty)
      logInfo(s"Partially supported filters (applied in AQL and Spark):\n${partialSupp.map(_._1).mkString("\n")}")
    if (noneSupp.nonEmpty)
      logInfo(s"Not supported filters (applied in Spark):\n${noneSupp.map(_._1).mkString("\n")}")

    (partialSupp ++ noneSupp).map(_._1)
  }

  override def pushedFilters(): Array[Filter] = appliedSparkFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
