package org.apache.spark.sql.arangodb.commons.exceptions

/**
 * Exception thrown after all writes attempts have failed.
 * It contains the exceptions thrown at each attempt.
 *
 * @param exceptions array of exceptions thrown at each attempt
 */
class ArangoDBDataWriterException(val exceptions: Array[Exception])
  extends RuntimeException(s"Failed ${exceptions.length} times: ${ArangoDBDataWriterException.toMessage(exceptions)}") {
  val attempts: Int = exceptions.length

  override def getCause: Throwable = exceptions(0)
}

private object ArangoDBDataWriterException {

  // creates exception message
  private def toMessage(exceptions: Array[Exception]): String = exceptions
    .zipWithIndex
    .map(it => s"""Attempt #${it._2 + 1}: ${it._1}""")
    .mkString("[\n\t", ",\n\t", "\n]")

}
