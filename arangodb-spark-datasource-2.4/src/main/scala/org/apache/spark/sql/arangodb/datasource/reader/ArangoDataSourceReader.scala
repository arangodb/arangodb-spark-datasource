package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.filter.{FilterSupport, PushableFilter}
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, PushdownUtils, ReadMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

class ArangoDataSourceReader(tableSchema: StructType, options: ArangoOptions) extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

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