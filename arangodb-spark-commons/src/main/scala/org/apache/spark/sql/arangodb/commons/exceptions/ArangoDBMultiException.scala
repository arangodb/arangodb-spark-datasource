package org.apache.spark.sql.arangodb.commons.exceptions

import com.arangodb.entity.ErrorEntity
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException.convert

// Due to https://github.com/scala/bug/issues/10679 scala Stream serialization could generate StackOverflowError. To
// avoid it we use:
//    val errors: Array[ErrorEntity]
// instead of:
//    val errors: Iterable[ErrorEntity]

/**
 * @param errors array of tuples with:
 *               _1 : the error entity
 *               _2 : the stringified record causing the error
 */
class ArangoDBMultiException(val errors: Array[(ErrorEntity, String)]) extends RuntimeException(convert(errors))

private object ArangoDBMultiException {
  private def convert(errors: Array[(ErrorEntity, String)]): String = errors
    .map(it => s"""Error: ${it._1.getErrorNum} - ${it._1.getErrorMessage} for record: ${it._2}""")
    .mkString("[\n\t", ",\n\t", "\n]")
}
