package org.apache.spark.sql.arangodb.commons.mapping

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

trait ArangoParser {

  /**
   * Parse the JSON or VPACK input to the set of [[InternalRow]]s.
   *
   * @param recordLiteral an optional function that will be used to generate
   *                      the corrupt record text instead of record.toString
   */
  def parse[T](
                record: T,
                createParser: (JsonFactory, T) => JsonParser,
                recordLiteral: T => UTF8String): Iterable[InternalRow]

  def parse(data: Array[Byte]): Iterable[InternalRow]

}
