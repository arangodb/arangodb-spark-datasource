package org.apache.spark.sql.arangodb.commons.exceptions

class ArangoDBDataWriterException(val exception: Exception, val attempts: Int = 1)
  extends RuntimeException(s"Failed $attempts times, most recent failure: $exception", exception)
