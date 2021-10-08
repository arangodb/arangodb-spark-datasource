package org.apache.spark.sql.arangodb.commons.exceptions

import com.arangodb.entity.ErrorEntity
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException.convert

class ArangoDBMultiException(val errors: Iterable[ErrorEntity]) extends RuntimeException(convert(errors))

private object ArangoDBMultiException {
  private def convert(errors: Iterable[ErrorEntity]): String = errors
    .map(it => s"""Error: ${it.getErrorNum} - ${it.getErrorMessage}""")
    .mkString("[\n\t", ",\n\t", "\n]")
}
