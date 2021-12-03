package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.filter.{FilterSupport, PushableFilter}
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, ReadMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

class ArangoDataSourceReader(tableSchema: StructType, options: ArangoOptions) extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  // fully or partially applied filters
  private var appliedPushableFilters: Array[PushableFilter] = Array()
  private var appliedSparkFilters: Array[Filter] = Array()

  private var requiredSchema: StructType = _

  override def readSchema(): StructType = Option(requiredSchema).getOrElse(tableSchema)

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = (options.readOptions.readMode match {
    case ReadMode.Query => List(new SingletonPartition(readSchema(), options)).asJava
    case ReadMode.Collection => planCollectionPartitions().toList.asJava
  }).asInstanceOf[util.List[InputPartition[InternalRow]]]

  private def planCollectionPartitions() =
    ArangoClient.getCollectionShardIds(options)
      .zip(Stream.continually(options.driverOptions.endpoints).flatten)
      .map(it => new ArangoCollectionPartition(it._1, it._2, new PushDownCtx(readSchema(), appliedPushableFilters), options))

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
      logInfo(s"Fully supported filters (applied in AQL):\n\t${fullSupp.map(_._1).mkString("\n\t")}")
    if (partialSupp.nonEmpty)
      logInfo(s"Partially supported filters (applied in AQL and Spark):\n\t${partialSupp.map(_._1).mkString("\n\t")}")
    if (noneSupp.nonEmpty)
      logInfo(s"Not supported filters (applied in Spark):\n\t${noneSupp.map(_._1).mkString("\n\t")}")

    (partialSupp ++ noneSupp).map(_._1)
  }

  override def pushedFilters(): Array[Filter] = appliedSparkFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

}