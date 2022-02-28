package org.apache.spark.sql.arangodb.commons.exceptions

import com.arangodb.entity.ErrorEntity
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException.convert

// Due to https://github.com/scala/bug/issues/10679 scala Stream serialization could generate StackOverflowError. To
// avoid it we use:
//    val errors: Array[ErrorEntity]
// instead of:
//    val errors: Iterable[ErrorEntity]
class ArangoDBMultiException(val errors: Array[ErrorEntity]) extends RuntimeException(convert(errors))

private object ArangoDBMultiException {
  private def convert(errors: Array[ErrorEntity]): String = errors
    .map(it => s"""Error: ${it.getErrorNum} - ${it.getErrorMessage}""")
    .mkString("[\n\t", ",\n\t", "\n]")
}
