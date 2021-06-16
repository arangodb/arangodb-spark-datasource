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
    // @formatter:off
    case f: And           => new AndFilter(apply(f.left, schema), apply(f.right, schema))
    case f: Or            => new OrFilter(apply(f.left, schema), apply(f.right, schema))
    case f: Not           => new NotFilter(apply(f.child, schema))
    case f: EqualTo       => new EqualToFilter(f.attribute, f.value, schema)
    case f: EqualNullSafe => new EqualToFilter(f.attribute, f.value, schema)
    case f: IsNull        => new IsNullFilter(f.attribute)
    case f: IsNotNull     => new IsNotNullFilter(f.attribute)
    case f: GreaterThan   => new GreaterThanFilter(f.attribute, f.value, schema)
    case _ => new PushableFilter {
      override def support(): FilterSupport = FilterSupport.NONE
      override def aql(documentVariable: String): String = throw new NotImplementedError()
    }
    // @formatter:on
  }
}

private class OrFilter(parts: PushableFilter*) extends PushableFilter {

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

private class AndFilter(parts: PushableFilter*) extends PushableFilter {

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

private class NotFilter(child: PushableFilter) extends PushableFilter {

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
    case t: DateType => s"""DATE_TIMESTAMP(`$v`.$escapedFieldName) == ${getValue(t, value)}"""
    case t: TimestampType => s"""DATE_TIMESTAMP(`$v`.$escapedFieldName) == ${getValue(t, value)}"""
    case t => s"""`$v`.$escapedFieldName == ${getValue(t, value)}"""
  }
}

private class GreaterThanFilter(attribute: String, value: Any, schema: StructType) extends PushableFilter {

  private val fieldNameParts = splitAttributeNameParts(attribute)
  private val dataType = getStructField(fieldNameParts.tail, schema(fieldNameParts.head)).dataType
  private val escapedFieldName = fieldNameParts.map(v => s"`$v`").mkString(".")

  override def support(): FilterSupport = dataType match {
    case _: TimestampType => FilterSupport.PARTIAL // microseconds are ignored in AQL
    case t if supportsType(t) => FilterSupport.FULL
    case _ => FilterSupport.NONE
  }

  override def aql(v: String): String = dataType match {
    case t: DateType => s"""DATE_TIMESTAMP(`$v`.$escapedFieldName) > ${getValue(t, value)}"""
    case t: TimestampType => s"""DATE_TIMESTAMP(`$v`.$escapedFieldName) > ${getValue(t, value)}"""
    case t => s"""`$v`.$escapedFieldName > ${getValue(t, value)}"""
  }
}

/**
 * matches null or missing fields
 */
private class IsNullFilter(attribute: String) extends PushableFilter {
  private val escapedFieldName = splitAttributeNameParts(attribute).map(v => s"`$v`").mkString(".")

  override def support(): FilterSupport = FilterSupport.FULL

  override def aql(v: String): String = s"`$v`.$escapedFieldName == null"
}

/**
 * matches defined and not null fields
 */
private class IsNotNullFilter(attribute: String) extends PushableFilter {
  private val escapedFieldName = splitAttributeNameParts(attribute).map(v => s"`$v`").mkString(".")

  override def support(): FilterSupport = FilterSupport.FULL

  override def aql(v: String): String = s"`$v`.$escapedFieldName != null"
}
