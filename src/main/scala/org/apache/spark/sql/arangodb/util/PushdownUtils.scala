package org.apache.spark.sql.arangodb.util

import org.apache.spark.sql.sources.{And, EqualTo, Filter, Or}
import org.apache.spark.sql.types.{DateType, StructField, StructType}

object PushdownUtils {

  private[util] def generateColumnsFilter(schema: StructType, documentVariable: String): String =
    doGenerateColumnsFilter(schema, s"`$documentVariable`.")

  private def doGenerateColumnsFilter(schema: StructType, ctx: String): String = "{" +
    schema.fields.map(generateFieldFilter(_, ctx)).mkString(",") + "}"

  private def generateFieldFilter(field: StructField, ctx: String): String = {
    val fieldName = s"`${field.name}`"
    val value = s"$ctx$fieldName"
    s"$fieldName:" + (field.dataType match {
      case s: StructType => doGenerateColumnsFilter(s, s"$value.")
      case _ => value
    })
  }

  def generateFilterClause(filters: Array[PushableFilter]): String = filters
    .filter(_.support != FilterSupport.NONE)
    .map(_.aql)
    .mkString(" AND ")

  def generateRowFilters(filters: Array[Filter], schema: StructType, documentVariable: String = "d"): Array[PushableFilter] =
    filters.map(generateRowFilter(_, schema, documentVariable))

  def generateRowFilter(filter: Filter, schema: StructType, documentVariable: String = "d"): PushableFilter = filter match {
    case or: Or =>
      val parts = Seq(
        generateRowFilter(or.left, schema, documentVariable),
        generateRowFilter(or.right, schema, documentVariable)
      )
      if (parts.exists(_.support == FilterSupport.NONE)) {
        PushableFilter(null, null, FilterSupport.NONE)
      } else {
        val supp = if (parts.forall(_.support == FilterSupport.FULL)) FilterSupport.FULL else FilterSupport.PARTIAL
        PushableFilter(or, s"(${parts(0)} OR ${parts(1)})", supp)
      }

    case and: And =>
      val parts = Seq(
        generateRowFilter(and.left, schema, documentVariable),
        generateRowFilter(and.right, schema, documentVariable)
      )
      if (parts.forall(_.support == FilterSupport.NONE)) {
        PushableFilter(null, null, FilterSupport.NONE)
      } else {
        val supp = if (parts.forall(_.support == FilterSupport.FULL)) FilterSupport.FULL else FilterSupport.PARTIAL
        PushableFilter(and, s"(${parts(0)} AND ${parts(1)})", supp)
      }

    // TODO:
    case filter: EqualTo => schema(filter.attribute).dataType match {
      case _: DateType => PushableFilter(null, null, FilterSupport.NONE)
      case _ => PushableFilter(
        filter,
        s"""
           |`$documentVariable`.`${filter.attribute}` == "${filter.value}"
           |""".stripMargin,
        FilterSupport.FULL
      )
    }

    case _ => PushableFilter(null, null, FilterSupport.NONE)
  }

}

case class PushableFilter(
                           filter: Filter,
                           aql: String,
                           support: FilterSupport
                         )

sealed trait FilterSupport

object FilterSupport {

  /**
   * the filter can be applied and does not need to be evaluated again after scanning
   */
  case object FULL extends FilterSupport

  /**
   * the filter can be partially applied and it needs to be evaluated again after scanning
   */
  case object PARTIAL extends FilterSupport

  /**
   * the filter cannot be applied and it needs to be evaluated again after scanning
   */
  case object NONE extends FilterSupport
}
