package org.apache.spark.sql.arangodb

import com.arangodb.velocypack.VPackParser
import org.apache.spark.sql.arangodb.datasource.ContentType
import org.apache.spark.sql.arangodb.util.mapping.{ArangoGenerator, ArangoParser}
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.io.ByteArrayOutputStream

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
      |  }
      |}
      |""".stripMargin.replaceAll("\\s", "")

  private val jsonBytes = jsonString.getBytes
  private val vpackBytes = new VPackParser.Builder().build().fromJson(jsonString).toByteArray

  private val schema: StructType = new StructType(
    Array(
      StructField("birthday", DateType),
      StructField("gender", StringType),
      StructField("likes", ArrayType(StringType)),
      StructField("name", StructType(
        Array(
          StructField("first", StringType),
          StructField("last", StringType),
        )
      ))
    )
  )

  @Test
  def jsonRoudTrip(): Unit = {
    roundTrip(ContentType.Json, jsonBytes)
  }

  @Test
  def vpackRoudTrip(): Unit = {
    roundTrip(ContentType.VPack, vpackBytes)
  }

  private def roundTrip(contentType: ContentType, data: Array[Byte]): Unit = {
    val parser = ArangoParser.of(contentType, schema)
    val parsed = parser.parse(data)
    val output = new ByteArrayOutputStream()
    val generator = ArangoGenerator.of(contentType, schema, output)
    generator.write(parsed.head)
    generator.close()
    assertThat(output.toByteArray).isEqualTo(data)
  }

}
