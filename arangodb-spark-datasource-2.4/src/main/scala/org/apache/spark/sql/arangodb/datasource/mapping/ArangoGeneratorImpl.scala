package org.apache.spark.sql.arangodb.datasource.mapping

import com.arangodb.jackson.dataformat.velocypack.VPackFactory
import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.arangodb.commons.ContentType
import org.apache.spark.sql.arangodb.datasource.mapping.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGenerator, ArangoGeneratorProvider}

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
  override def of(contentType: ContentType, schema: StructType, outputStream: OutputStream): ArangoGeneratorImpl = contentType match {
    case ContentType.JSON => new JsonArangoGenerator(schema, outputStream)
    case ContentType.VPACK => new VPackArangoGenerator(schema, outputStream)
    case _ => throw new IllegalArgumentException
  }
}

class JsonArangoGenerator(schema: StructType, outputStream: OutputStream)
  extends ArangoGeneratorImpl(
    schema,
    outputStream,
    createOptions(new JsonFactory())
  )

class VPackArangoGenerator(schema: StructType, outputStream: OutputStream)
  extends ArangoGeneratorImpl(
    schema,
    outputStream,
    createOptions(new VPackFactory())
  )
