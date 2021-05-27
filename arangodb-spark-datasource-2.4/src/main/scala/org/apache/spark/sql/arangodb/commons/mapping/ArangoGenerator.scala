package org.apache.spark.sql.arangodb.commons.mapping

import com.arangodb.jackson.dataformat.velocypack.VPackFactoryBuilder
import com.fasterxml.jackson.core.JsonFactoryBuilder
import org.apache.spark.sql.arangodb.commons.ContentType
import org.apache.spark.sql.arangodb.util.mapping.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.OutputStream

abstract sealed class ArangoGenerator(
                                       schema: DataType,
                                       writer: OutputStream,
                                       options: JSONOptions)
  extends JacksonGenerator(
    schema,
    options.buildJsonFactory().createGenerator(writer),
    options)

object ArangoGenerator {
  def of(contentType: ContentType, schema: StructType, outputStream: OutputStream): ArangoGenerator = contentType match {
    case ContentType.Json => new JsonArangoGenerator(schema, outputStream)
    case ContentType.VPack => new VPackArangoGenerator(schema, outputStream)
    case _ => throw new IllegalArgumentException
  }
}

class JsonArangoGenerator(schema: StructType, outputStream: OutputStream)
  extends ArangoGenerator(
    schema,
    outputStream,
    createOptions(new JsonFactoryBuilder().build())
  )

class VPackArangoGenerator(schema: StructType, outputStream: OutputStream)
  extends ArangoGenerator(
    schema,
    outputStream,
    createOptions(new VPackFactoryBuilder().build())
  )
