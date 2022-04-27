package org.apache.spark.sql.arangodb.datasource

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.arangodb.datasource.mapping.json.JSONOptions

package object mapping {
  private[mapping] def createOptions(jsonFactory: JsonFactory, conf: Map[String, String]) =
    new JSONOptions(conf, "UTC") {
      override def buildJsonFactory(): JsonFactory = jsonFactory
    }
}
