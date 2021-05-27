package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoOptions, FilterSupport, PushdownUtils, ReadMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

class ArangoDataSourceReader(schema: StructType, options: ArangoOptions) extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  // fully or partially applied filters
  private var appliedFilters: Array[Filter] = Array()

  // partially or not applied filters
  private var toEvaluateFilters: Array[Filter] = Array()

  private var requiredSchema: StructType = _

  override def readSchema(): StructType = Option(requiredSchema).getOrElse(schema)

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = (options.readOptions.readMode match {
    case ReadMode.Query => List(new SingletonPartition(readSchema(), appliedFilters, options)).asJava
    case ReadMode.Collection => planCollectionPartitions().toList.asJava
  }).asInstanceOf[util.List[InputPartition[InternalRow]]]

  private def planCollectionPartitions() =
    ArangoClient.getCollectionShardIds(options)
      .zip(Stream.continually(options.driverOptions.endpoints).flatten)
      .map(it => new ArangoCollectionPartition(it._1, it._2, readSchema(), appliedFilters, options))

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    appliedFilters = filters.filter(PushdownUtils.generateRowFilter(_, schema).support != FilterSupport.NONE)
    toEvaluateFilters = filters.filter(PushdownUtils.generateRowFilter(_, schema).support != FilterSupport.FULL)
    toEvaluateFilters
  }

  override def pushedFilters(): Array[Filter] = appliedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

}