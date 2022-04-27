package org.apache.spark.sql.arangodb.commons.mapping

import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, ContentType}
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.util.ServiceLoader

trait ArangoGeneratorProvider {
  def of(contentType: ContentType, schema: StructType, outputStream: OutputStream, conf: ArangoDBConf): ArangoGenerator
}

object ArangoGeneratorProvider {
  def apply(): ArangoGeneratorProvider = ServiceLoader.load(classOf[ArangoGeneratorProvider]).iterator().next()
}
