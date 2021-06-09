package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.arangodb.commons.filter.{FilterSupport, PushableFilter}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

import scala.annotation.tailrec

// FIXME: use documentVariable instead of "d"
object PushdownUtils {

  private[commons] def generateColumnsFilter(schema: StructType, documentVariable: String): String =
    doGenerateColumnsFilter(schema, s"`$documentVariable`.")

  private def doGenerateColumnsFilter(schema: StructType, ctx: String): String =
    s"""{${schema.fields.map(generateFieldFilter(_, ctx)).mkString(",")}}"""

  private def generateFieldFilter(field: StructField, ctx: String): String = {
    val fieldName = s"`${field.name}`"
    val value = s"$ctx$fieldName"
    s"$fieldName:" + (field.dataType match {
      case s: StructType => doGenerateColumnsFilter(s, s"$value.")
      case _ => value
    })
  }

  def generateFilterClause(filters: Array[PushableFilter]): String = filters match {
    case Array() => ""
    case _ => "FILTER " + filters
      .filter(_.support != FilterSupport.NONE)
      .map(_.aql("d"))
      .mkString(" AND ")
  }

  def generateRowFilters(filters: Array[Filter], schema: StructType, documentVariable: String = "d"): Array[PushableFilter] =
    filters.map(PushableFilter(_, schema))

  @tailrec
  private[commons] def getStructField(fieldNameParts: Array[String], fieldSchema: StructField): StructField = fieldNameParts match {
    case Array() => fieldSchema
    case _ => getStructField(fieldNameParts.tail, fieldSchema.dataType.asInstanceOf[StructType](fieldNameParts.head))
  }

}
