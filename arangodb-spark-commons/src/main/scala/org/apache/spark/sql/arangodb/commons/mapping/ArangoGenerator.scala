package org.apache.spark.sql.arangodb.commons.mapping

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}

trait ArangoGenerator {

  def close(): Unit

  def flush(): Unit

  def writeStartArray(): Unit

  def writeEndArray(): Unit

  /**
   * Transforms a single `InternalRow` to JSON or VPACK object.
   *
   * @param row The row to convert
   */
  def write(row: InternalRow): Unit

  /**
   * Transforms multiple `InternalRow`s or `MapData`s to JSON or VPACK array
   *
   * @param array The array of rows or maps to convert
   */
  def write(array: ArrayData): Unit

  /**
   * Transforms a single `MapData` to JSON or VPACK object
   *
   * @param map a map to convert
   */
  def write(map: MapData): Unit

  def writeLineEnding(): Unit
}
