package org.apache.spark.sql.arangodb.commons.exceptions

class DataWriteAbortException(message: String) extends RuntimeException(message) {
  def this() = this("Aborted")
}
