package org.apache.spark.sql.arangodb.datasource

import com.arangodb.serde.JacksonSerde
import com.arangodb.spark.DefaultSource
import com.arangodb.{ArangoDB, ArangoDatabase, DbName}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.sql.Date
import java.time.LocalDate
import java.util.Base64
import javax.net.ssl.{SSLContext, TrustManagerFactory}

@EnabledIfSystemProperty(named = "SslTest", matches = "true")
class SslTest {
  private val arangoDB: ArangoDB = SslTest.arangoDB
  private val db: ArangoDatabase = SslTest.db
  private val spark: SparkSession = SslTest.spark
  private val options: Map[String, String] = SslTest.options

  @BeforeEach
  def startup(): Unit = {
    println(arangoDB.getVersion.getVersion)
    val col = db.collection("sslTest")
    if (!col.exists()) col.create()

    import scala.collection.JavaConverters.seqAsJavaListConverter

    SslTest.createDF(
      "sslTest",
      Seq(
        Map(
          "str" -> "s1",
          "int" -> 1
        ),
        Map(
          "str" -> "s2",
          "int" -> 2
        ),
        Map(
          "str" -> "s3",
          "int" -> 3
        )
      ).asInstanceOf[Seq[Any]].asJava,
      StructType(Array(
        StructField("str", StringType),
        StructField("int", IntegerType)
      ))
    )
  }

  @AfterEach
  def shutdown(): Unit = {
    db.collection("sslTest").drop()
    arangoDB.shutdown()
  }


  @Test
  def sslTest(): Unit = {
    val df = spark.read
      .format(classOf[DefaultSource].getName)
      .options(options + ("table" -> "sslTest"))
      .load()
    df.show()
  }

}

object SslTest {

  private val database = "sparkConnectorTest"
  private val user = "root"
  private val password = "test"
  private val endpoints = "localhost:8529"
  private val singleEndpoint = endpoints.split(',').head
  private val b64cert = "MIIDezCCAmOgAwIBAgIEeDCzXzANBgkqhkiG9w0BAQsFADBuMRAwDgYDVQQGEwdVbmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYDVQQKEwdVbmtub3duMRAwDgYDVQQLEwdVbmtub3duMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMjAxMTAxMTg1MTE5WhcNMzAxMDMwMTg1MTE5WjBuMRAwDgYDVQQGEwdVbmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYDVQQKEwdVbmtub3duMRAwDgYDVQQLEwdVbmtub3duMRIwEAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC1WiDnd4+uCmMG539ZNZB8NwI0RZF3sUSQGPx3lkqaFTZVEzMZL76HYvdc9Qg7difyKyQ09RLSpMALX9euSseD7bZGnfQH52BnKcT09eQ3wh7aVQ5sN2omygdHLC7X9usntxAfv7NzmvdogNXoJQyY/hSZff7RIqWH8NnAUKkjqOe6Bf5LDbxHKESmrFBxOCOnhcpvZWetwpiRdJVPwUn5P82CAZzfiBfmBZnB7D0l+/6Cv4jMuH26uAIcixnVekBQzl1RgwczuiZf2MGO64vDMMJJWE9ClZF1uQuQrwXF6qwhuP1Hnkii6wNbTtPWlGSkqeutr004+Hzbf8KnRY4PAgMBAAGjITAfMB0GA1UdDgQWBBTBrv9Awynt3C5IbaCNyOW5v4DNkTANBgkqhkiG9w0BAQsFAAOCAQEAIm9rPvDkYpmzpSIhR3VXG9Y71gxRDrqkEeLsMoEyqGnw/zx1bDCNeGg2PncLlW6zTIipEBooixIE9U7KxHgZxBy0Et6EEWvIUmnr6F4F+dbTD050GHlcZ7eOeqYTPYeQC502G1Fo4tdNi4lDP9L9XZpf7Q1QimRH2qaLS03ZFZa2tY7ah/RQqZL8Dkxx8/zc25sgTHVpxoK853glBVBs/ENMiyGJWmAXQayewY3EPt/9wGwV4KmU3dPDleQeXSUGPUISeQxFjy+jCw21pYviWVJTNBA9l5ny3GhEmcnOT/gQHCvVRLyGLMbaMZ4JrPwb+aAtBgrgeiK4xeSMMvrbhw=="
  private val arangoDB: ArangoDB = {
    val serde = JacksonSerde.of(com.arangodb.ContentType.JSON)
    serde.configure(mapper =>
      mapper
        .registerModule(DefaultScalaModule)
        .registerModule(new SimpleModule()
          .addSerializer(classOf[Date], new JsonSerializer[Date] {
            override def serialize(value: Date, gen: JsonGenerator, serializers: SerializerProvider): Unit =
              gen.writeString(value.toString)
          })
          .addSerializer(classOf[LocalDate], new JsonSerializer[LocalDate] {
            override def serialize(value: LocalDate, gen: JsonGenerator, serializers: SerializerProvider): Unit =
              gen.writeString(value.toString)
          })
        )
    )

    new ArangoDB.Builder()
      .useSsl(true)
      .sslContext({
        val is = new ByteArrayInputStream(Base64.getDecoder.decode(b64cert))
        val cert = CertificateFactory.getInstance("X.509").generateCertificate(is)
        val ks = KeyStore.getInstance(KeyStore.getDefaultType)
        ks.load(null)
        ks.setCertificateEntry("arangodb", cert)
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(ks)
        val sc = SSLContext.getInstance("TLS")
        sc.init(null, tmf.getTrustManagers, null)
        sc
      })
      .user(user)
      .password(password)
      .host(singleEndpoint.split(':').head, singleEndpoint.split(':')(1).toInt)
      .serde(serde)
      .build()
  }
  private val db: ArangoDatabase = createDB()
  private val options = Map(
    "ssl.enabled" -> "true",
    "ssl.cert.value" -> b64cert,
    "database" -> database,
    "user" -> user,
    "password" -> password,
    "endpoints" -> endpoints
  )

  private val spark: SparkSession = SparkSession.builder()
    .appName("ArangoDBSparkTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()

  private def createDB(): ArangoDatabase = {
    val db = arangoDB.db(DbName.of(database))
    if (!db.exists()) db.create()
    db
  }

  def createDF(name: String, docs: java.util.Collection[Any], schema: StructType, dropExisting: Boolean = true): DataFrame = {
    val col = db.collection(name)
    if (col.exists()) {
      if (dropExisting) {
        col.drop()
        col.create()
      }
    } else {
      col.create()
    }
    col.insertDocuments(docs)

    val df = spark.read
      .format(classOf[DefaultSource].getName)
      .options(options + ("table" -> name))
      .schema(schema)
      .load()
    df.createOrReplaceTempView(name)
    df
  }

  def dropTable(name: String): Unit = {
    db.collection(name).drop()
  }
}
