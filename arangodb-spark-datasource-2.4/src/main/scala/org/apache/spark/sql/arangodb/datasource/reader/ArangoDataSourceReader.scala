package org.apache.spark.sql.arangodb.datasource.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.arangodb.commons.filter.{FilterSupport, PushableFilter}
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx
import org.apache.spark.sql.arangodb.commons.{ArangoClient, ArangoDBConf, ReadMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{StringType, StructType}

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

class ArangoDataSourceReader(tableSchema: StructType, options: ArangoDBConf) extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  verifyColumnNameOfCorruptRecord(tableSchema, options.readOptions.columnNameOfCorruptRecord)

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

  /**
   * A convenient function for schema validation in datasources supporting
   * `columnNameOfCorruptRecord` as an option.
   */
  private def verifyColumnNameOfCorruptRecord(
                                               schema: StructType,
                                               columnNameOfCorruptRecord: String): Unit = {
    schema.getFieldIndex(columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = schema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw new AnalysisException(
          "The field for corrupt records must be string type and nullable")
      }
    }
  }

}