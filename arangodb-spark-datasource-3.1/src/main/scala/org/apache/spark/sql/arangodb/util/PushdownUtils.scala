package org.apache.spark.sql.arangodb.util

import org.apache.spark.sql.sources.{And, EqualTo, Filter, Or}
import org.apache.spark.sql.types.{DateType, StructField, StructType}

import scala.annotation.tailrec

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

    /*
     * +---------++---------+---------+------+
     * |   OR    ||  FULL   | PARTIAL | NONE |
     * +---------++---------+---------+------+
     * | FULL    || FULL    | PARTIAL | NONE |
     * | PARTIAL || PARTIAL | PARTIAL | NONE |
     * | NONE    || NONE    | NONE    | NONE |
     * +---------++---------+---------+------+
     */
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

    /*
     * +---------++---------+---------+---------+
     * |   AND   ||  FULL   | PARTIAL |  NONE   |
     * +---------++---------+---------+---------+
     * | FULL    || FULL    | PARTIAL | PARTIAL |
     * | PARTIAL || PARTIAL | PARTIAL | PARTIAL |
     * | NONE    || PARTIAL | PARTIAL | NONE    |
     * +---------++---------+---------+---------+
     */
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

    // TODO: check support foreach field dataType
    case filter: EqualTo => {
      // FIXME: don't split quoted parts, according to the doc {@link org.apache.spark.sql.sources.EqualTo}:
      //        `dots` are used as separators for nested columns. If any part of the names contains `dots`,
      //         it is quoted to avoid confusion.
      val fieldNameParts = filter.attribute.split('.')
      val schemaField = getStructField(fieldNameParts.tail, schema(fieldNameParts.head))
      val escapedFieldNameParts = fieldNameParts.map(v => s"`$v`").mkString(".")
      schemaField.dataType match {
        case _: DateType => PushableFilter(null, null, FilterSupport.NONE)
        case _ => PushableFilter(
          filter,
          s"""
             |`$documentVariable`.$escapedFieldNameParts == "${filter.value}"
             |""".stripMargin,
          FilterSupport.FULL
        )
      }
    }

    case _ => PushableFilter(null, null, FilterSupport.NONE)
  }

  @tailrec
  private[util] def getStructField(fieldNameParts: Array[String], fieldSchema: StructField): StructField = fieldNameParts match {
    case Array() => fieldSchema
    case _ => getStructField(fieldNameParts.tail, fieldSchema.dataType.asInstanceOf[StructType](fieldNameParts.head))
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
