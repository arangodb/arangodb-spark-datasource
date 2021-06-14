package org.apache.spark.sql.arangodb.datasource.mapping

import com.arangodb.jackson.dataformat.velocypack.VPackFactory
import com.arangodb.velocypack.{VPackParser, VPackSlice}
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.sql.arangodb.commons.ContentType
import org.apache.spark.sql.arangodb.datasource.mapping.json.{JSONOptions, JacksonParser}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

abstract sealed class ArangoParser(
                                    schema: DataType,
                                    options: JSONOptions,
                                    recordLiteral: Array[Byte] => UTF8String)
  extends JacksonParser(schema, options) {
  def parse(data: Array[Byte]): Iterable[InternalRow] = super.parse(
    data,
    (jsonFactory: JsonFactory, record: Array[Byte]) => jsonFactory.createParser(record),
    recordLiteral
  )
}

object ArangoParser {
  def of(contentType: ContentType, schema: DataType): ArangoParser = contentType match {
    case ContentType.Json => new JsonArangoParser(schema)
    case ContentType.VPack => new VPackArangoParser(schema)
    case _ => throw new IllegalArgumentException
  }
}

class JsonArangoParser(schema: DataType)
  extends ArangoParser(
    schema,
    createOptions(new JsonFactory()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)),
    (bytes: Array[Byte]) => UTF8String.fromBytes(bytes)
  )

class VPackArangoParser(schema: DataType)
  extends ArangoParser(
    schema,
    createOptions(new VPackFactory()),
    (bytes: Array[Byte]) => UTF8String.fromString(new VPackParser.Builder().build().toJson(new VPackSlice(bytes)))
  )
