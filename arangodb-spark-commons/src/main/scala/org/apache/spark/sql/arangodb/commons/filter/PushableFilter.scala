package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.arangodb.commons.PushdownUtils.getStructField
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DateType, StructType, TimestampType}

sealed trait PushableFilter extends Serializable {
  def support(): FilterSupport

  def aql(documentVariable: String): String
}

object PushableFilter {
  def apply(filter: Filter, schema: StructType): PushableFilter = filter match {
    case and: And => new AndFilter(and, schema)
    case or: Or => new OrFilter(or, schema)
    case not: Not => new NotFilter(not, schema)
    case equalTo: EqualTo => new EqualToFilter(equalTo.attribute, equalTo.value, schema)
    case equalNullSafe: EqualNullSafe => new EqualToFilter(equalNullSafe.attribute, equalNullSafe.value, schema)
    case isNull: IsNull => new IsNullFilter(isNull)
    case isNotNull: IsNotNull => new IsNotNullFilter(isNotNull)
    case _ => new PushableFilter {
      override def support(): FilterSupport = FilterSupport.NONE

      override def aql(documentVariable: String): String = throw new NotImplementedError()
    }
  }
}

private class OrFilter(or: Or, schema: StructType) extends PushableFilter {
  private val parts = Seq(
    PushableFilter(or.left, schema),
    PushableFilter(or.right, schema)
  )

  /**
   * +---------++---------+---------+------+
   * |   OR    ||  FULL   | PARTIAL | NONE |
   * +---------++---------+---------+------+
   * | FULL    || FULL    | PARTIAL | NONE |
   * | PARTIAL || PARTIAL | PARTIAL | NONE |
   * | NONE    || NONE    | NONE    | NONE |
   * +---------++---------+---------+------+
   */
  override def support(): FilterSupport =
    if (parts.exists(_.support == FilterSupport.NONE)) FilterSupport.NONE
    else if (parts.forall(_.support == FilterSupport.FULL)) FilterSupport.FULL
    else FilterSupport.PARTIAL

  override def aql(v: String): String = parts
    .map(_.aql(v))
    .mkString("(", " OR ", ")")
}

private class AndFilter(and: And, schema: StructType) extends PushableFilter {
  private val parts = Seq(
    PushableFilter(and.left, schema),
    PushableFilter(and.right, schema)
  )

  /**
   * +---------++---------+---------+---------+
   * |   AND   ||  FULL   | PARTIAL |  NONE   |
   * +---------++---------+---------+---------+
   * | FULL    || FULL    | PARTIAL | PARTIAL |
   * | PARTIAL || PARTIAL | PARTIAL | PARTIAL |
   * | NONE    || PARTIAL | PARTIAL | NONE    |
   * +---------++---------+---------+---------+
   */
  override def support(): FilterSupport =
    if (parts.forall(_.support == FilterSupport.NONE)) FilterSupport.NONE
    else if (parts.forall(_.support == FilterSupport.FULL)) FilterSupport.FULL
    else FilterSupport.PARTIAL

  override def aql(v: String): String = parts
    .filter(_.support() != FilterSupport.NONE)
    .map(_.aql(v))
    .mkString("(", " AND ", ")")
}

private class NotFilter(not: Not, schema: StructType) extends PushableFilter {
  private val child = PushableFilter(not.child, schema)

  /**
   * +---------++---------+
   * |   v     || NOT(v)  |
   * +---------++---------+
   * | FULL    || FULL    |
   * | PARTIAL || NONE    |
   * | NONE    || NONE    |
   * +---------++---------+
   */
  override def support(): FilterSupport =
    if (child.support() == FilterSupport.FULL) FilterSupport.FULL
    else FilterSupport.NONE

  override def aql(v: String): String = s"NOT (${child.aql(v)})"
}

private class EqualToFilter(attribute: String, value: Any, schema: StructType) extends PushableFilter {

  private val fieldNameParts = splitAttributeNameParts(attribute)
  private val dataType = getStructField(fieldNameParts.tail, schema(fieldNameParts.head)).dataType
  private val escapedFieldName = fieldNameParts.map(v => s"`$v`").mkString(".")

  override def support(): FilterSupport = dataType match {
    case _: TimestampType => FilterSupport.PARTIAL // microseconds are ignored in AQL
    case t if supportsType(t) => FilterSupport.FULL
    case _ => FilterSupport.NONE
  }

  override def aql(v: String): String = dataType match {
    case t: DateType => s"""DATE_COMPARE(`$v`.$escapedFieldName, ${getValue(t, value)}, "years", "days")"""
    case t: TimestampType => s"""DATE_COMPARE(`$v`.$escapedFieldName, ${getValue(t, value)}, "years", "milliseconds")"""
    case t => s"""`$v`.$escapedFieldName == ${getValue(t, value)}"""
  }
}

/**
 * @note matches null or missing fields
 */
private class IsNullFilter(filter: IsNull) extends PushableFilter {
  private val escapedFieldName = splitAttributeNameParts(filter.attribute).map(v => s"`$v`").mkString(".")

  override def support(): FilterSupport = FilterSupport.FULL

  override def aql(v: String): String = s"`$v`.$escapedFieldName == null"
}

private class IsNotNullFilter(filter: IsNotNull) extends PushableFilter {
  private val escapedFieldName = splitAttributeNameParts(filter.attribute).map(v => s"`$v`").mkString(".")

  override def support(): FilterSupport = FilterSupport.FULL

  override def aql(v: String): String = s"`$v`.$escapedFieldName != null"
}
