package org.apache.spark.sql.arangodb.commons.exceptions

import com.arangodb.entity.ErrorEntity

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
class ArangoDBMultiException(val errors: Array[(ErrorEntity, String)])
  extends RuntimeException(ArangoDBMultiException.toMessage(errors))

private object ArangoDBMultiException {

  // creates exception message keeping only 5 errors for each error type
  private def toMessage(errors: Array[(ErrorEntity, String)]): String = errors
    .groupBy(_._1.getErrorNum)
    .mapValues(_.take(5))
    .values
    .flatten
    .map(it => s"""Error: ${it._1.getErrorNum} - ${it._1.getErrorMessage} for record: ${it._2}""")
    .mkString("[\n\t", ",\n\t", "\n]")
}
