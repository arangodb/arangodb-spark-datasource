package org.apache.spark.sql.arangodb.datasource.mapping

import com.arangodb.jackson.dataformat.velocypack.VPackFactoryBuilder
import com.fasterxml.jackson.core.JsonFactoryBuilder
import org.apache.spark.sql.arangodb.commons.ContentType
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGenerator, ArangoGeneratorProvider}
import org.apache.spark.sql.arangodb.datasource.mapping.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.OutputStream

abstract sealed class ArangoGeneratorImpl(
                                           schema: DataType,
                                           writer: OutputStream,
                                           options: JSONOptions)
  extends JacksonGenerator(
    schema,
    options.buildJsonFactory().createGenerator(writer),
    options) with ArangoGenerator

class ArangoGeneratorProviderImpl extends ArangoGeneratorProvider {
  override def of(
                   contentType: ContentType,
                   schema: StructType,
                   outputStream: OutputStream,
                   conf: Map[String, String]
                 ): ArangoGeneratorImpl = contentType match {
    case ContentType.JSON => new JsonArangoGenerator(schema, outputStream, conf: Map[String, String])
    case ContentType.VPACK => new VPackArangoGenerator(schema, outputStream, conf: Map[String, String])
    case _ => throw new IllegalArgumentException
  }
}

class JsonArangoGenerator(schema: StructType, outputStream: OutputStream, conf: Map[String, String])
  extends ArangoGeneratorImpl(
    schema,
    outputStream,
    createOptions(new JsonFactoryBuilder().build(), conf)
  )

class VPackArangoGenerator(schema: StructType, outputStream: OutputStream, conf: Map[String, String])
  extends ArangoGeneratorImpl(
    schema,
    outputStream,
    createOptions(new VPackFactoryBuilder().build(), conf)
  )
