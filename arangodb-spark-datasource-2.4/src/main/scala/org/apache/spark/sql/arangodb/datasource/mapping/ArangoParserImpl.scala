package org.apache.spark.sql.arangodb.datasource.mapping

import com.arangodb.jackson.dataformat.velocypack.VPackFactory
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.sql.arangodb.commons.{ArangoDBConf, ContentType}
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoParser, ArangoParserProvider, MappingUtils}
import org.apache.spark.sql.arangodb.datasource.mapping.json.{JSONOptions, JacksonParser}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

abstract sealed class ArangoParserImpl(
                                        schema: DataType,
                                        options: JSONOptions,
                                        recordLiteral: Array[Byte] => UTF8String)
  extends JacksonParser(schema, options) with ArangoParser {
  def parse(data: Array[Byte]): Seq[InternalRow] = super.parse(
    data,
    (jsonFactory: JsonFactory, record: Array[Byte]) => jsonFactory.createParser(record),
    recordLiteral
  )
}

class ArangoParserProviderImpl extends ArangoParserProvider {
  override def of(contentType: ContentType, schema: DataType, conf: ArangoDBConf): ArangoParserImpl = contentType match {
    case ContentType.JSON => new JsonArangoParser(schema, conf)
    case ContentType.VPACK => new VPackArangoParser(schema, conf)
    case _ => throw new IllegalArgumentException
  }
}

class JsonArangoParser(schema: DataType, conf: ArangoDBConf)
  extends ArangoParserImpl(
    schema,
    createOptions(new JsonFactory().configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true), conf),
    (bytes: Array[Byte]) => UTF8String.fromBytes(bytes)
  )

class VPackArangoParser(schema: DataType, conf: ArangoDBConf)
  extends ArangoParserImpl(
    schema,
    createOptions(new VPackFactory(), conf),
    (bytes: Array[Byte]) => UTF8String.fromString(MappingUtils.vpackToJson(bytes))
  )
