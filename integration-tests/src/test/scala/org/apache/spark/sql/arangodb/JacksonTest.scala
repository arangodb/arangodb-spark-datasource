package org.apache.spark.sql.arangodb

import com.arangodb.velocypack.VPackParser
import org.apache.spark.sql.arangodb.commons.ContentType
import org.apache.spark.sql.arangodb.commons.mapping.{ArangoGeneratorProvider, ArangoParserProvider}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

/**
 * @author Michele Rastelli
 */
class JacksonTest {
  private val jsonString =
    """
      |{
      |  "birthday": "1964-01-02",
      |  "gender": "female",
      |  "likes": [
      |    "swimming"
      |  ],
      |  "name": {
      |    "first": "Roseline",
      |    "last": "Jucean"
      |  },
      |  "nullString": null,
      |  "nullField": null,
      |  "mapField": {
      |    "foo": 1,
      |    "bar": 2
      |  }
      |}
      |""".stripMargin.replaceAll("\\s", "")

  private val jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8)
  private val vpackBytes = new VPackParser.Builder().build().fromJson(jsonString, true).toByteArray

  private val schema: StructType = new StructType(
    Array(
      StructField("birthday", DateType),
      StructField("gender", StringType),
      StructField("likes", ArrayType(StringType)),
      StructField("name", StructType(
        Array(
          StructField("first", StringType),
          StructField("last", StringType)
        )
      )),
      StructField("nullString", StringType, nullable = true),
      StructField("nullField", NullType),
      StructField("mapField", MapType(StringType, IntegerType))
    )
  )

  @Test
  def jsonRoudTrip(): Unit = {
    roundTrip(ContentType.JSON, jsonBytes)
  }

  @Test
  def vpackRoudTrip(): Unit = {
    roundTrip(ContentType.VPACK, vpackBytes)
  }

  private def roundTrip(contentType: ContentType, data: Array[Byte]): Unit = {
    val parser = ArangoParserProvider().of(contentType, schema, Map.empty)
    val parsed = parser.parse(data)
    val output = new ByteArrayOutputStream()
    val generator = ArangoGeneratorProvider().of(contentType, schema, output ,Map.empty)
    generator.write(parsed.head)
    generator.close()
    assertThat(output.toByteArray).isEqualTo(data)
  }

}
