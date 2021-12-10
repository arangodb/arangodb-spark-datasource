package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.arangodb.commons.filter.{FilterSupport, PushableFilter}
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ArangoScanBuilder(options: ArangoDBConf, tableSchema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  private var requiredSchema: StructType = _

  // fully or partially applied filters
  private var appliedPushableFilters: Array[PushableFilter] = Array()
  private var appliedSparkFilters: Array[Filter] = Array()

  override def build(): Scan = new ArangoScan(new PushDownCtx(requiredSchema, appliedPushableFilters), options)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // filters related to columnNameOfCorruptRecord are not pushed down
    val isCorruptRecordFilter = (f: Filter) => f.references.contains(options.readOptions.columnNameOfCorruptRecord)
    val ignoredFilters = filters.filter(isCorruptRecordFilter)
    val filtersBySupport = filters
      .filterNot(isCorruptRecordFilter)
      .map(f => (f, PushableFilter(f, tableSchema)))
      .groupBy(_._2.support())

    val fullSupp = filtersBySupport.getOrElse(FilterSupport.FULL, Array())
    val partialSupp = filtersBySupport.getOrElse(FilterSupport.PARTIAL, Array())
    val noneSupp = filtersBySupport.getOrElse(FilterSupport.NONE, Array()).map(_._1) ++ ignoredFilters

    val appliedFilters = fullSupp ++ partialSupp
    appliedPushableFilters = appliedFilters.map(_._2)
    appliedSparkFilters = appliedFilters.map(_._1)

    if (fullSupp.nonEmpty)
      logInfo(s"Filters fully applied in AQL:\n\t${fullSupp.map(_._1).mkString("\n\t")}")
    if (partialSupp.nonEmpty)
      logInfo(s"Filters partially applied in AQL:\n\t${partialSupp.map(_._1).mkString("\n\t")}")
    if (noneSupp.nonEmpty)
      logInfo(s"Filters not applied in AQL:\n\t${noneSupp.mkString("\n\t")}")

    partialSupp.map(_._1) ++ noneSupp
  }

  override def pushedFilters(): Array[Filter] = appliedSparkFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
