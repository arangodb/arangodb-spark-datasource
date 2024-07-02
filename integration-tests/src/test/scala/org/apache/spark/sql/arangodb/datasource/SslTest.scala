package org.apache.spark.sql.arangodb.datasource

import com.arangodb.serde.jackson.JacksonSerde
import com.arangodb.spark.DefaultSource
import com.arangodb.{ArangoDB, ArangoDatabase}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
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
import java.util.function.Consumer
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
  private val endpoints = "172.28.0.1:8529"
  private val singleEndpoint = endpoints.split(',').head
  private val b64cert = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURlekNDQW1PZ0F3SUJBZ0lFZURDelh6QU5CZ2txaGtpRzl3MEJBUXNGQURCdU1SQXdEZ1lEVlFRR0V3ZFYKYm10dWIzZHVNUkF3RGdZRFZRUUlFd2RWYm10dWIzZHVNUkF3RGdZRFZRUUhFd2RWYm10dWIzZHVNUkF3RGdZRApWUVFLRXdkVmJtdHViM2R1TVJBd0RnWURWUVFMRXdkVmJtdHViM2R1TVJJd0VBWURWUVFERXdsc2IyTmhiR2h2CmMzUXdIaGNOTWpBeE1UQXhNVGcxTVRFNVdoY05NekF4TURNd01UZzFNVEU1V2pCdU1SQXdEZ1lEVlFRR0V3ZFYKYm10dWIzZHVNUkF3RGdZRFZRUUlFd2RWYm10dWIzZHVNUkF3RGdZRFZRUUhFd2RWYm10dWIzZHVNUkF3RGdZRApWUVFLRXdkVmJtdHViM2R1TVJBd0RnWURWUVFMRXdkVmJtdHViM2R1TVJJd0VBWURWUVFERXdsc2IyTmhiR2h2CmMzUXdnZ0VpTUEwR0NTcUdTSWIzRFFFQkFRVUFBNElCRHdBd2dnRUtBb0lCQVFDMVdpRG5kNCt1Q21NRzUzOVoKTlpCOE53STBSWkYzc1VTUUdQeDNsa3FhRlRaVkV6TVpMNzZIWXZkYzlRZzdkaWZ5S3lRMDlSTFNwTUFMWDlldQpTc2VEN2JaR25mUUg1MkJuS2NUMDllUTN3aDdhVlE1c04yb215Z2RITEM3WDl1c250eEFmdjdOem12ZG9nTlhvCkpReVkvaFNaZmY3UklxV0g4Tm5BVUtranFPZTZCZjVMRGJ4SEtFU21yRkJ4T0NPbmhjcHZaV2V0d3BpUmRKVlAKd1VuNVA4MkNBWnpmaUJmbUJabkI3RDBsKy82Q3Y0ak11SDI2dUFJY2l4blZla0JRemwxUmd3Y3p1aVpmMk1HTwo2NHZETU1KSldFOUNsWkYxdVF1UXJ3WEY2cXdodVAxSG5raWk2d05iVHRQV2xHU2txZXV0cjAwNCtIemJmOEtuClJZNFBBZ01CQUFHaklUQWZNQjBHQTFVZERnUVdCQlRCcnY5QXd5bnQzQzVJYmFDTnlPVzV2NEROa1RBTkJna3EKaGtpRzl3MEJBUXNGQUFPQ0FRRUFJbTlyUHZEa1lwbXpwU0loUjNWWEc5WTcxZ3hSRHJxa0VlTHNNb0V5cUdudwovengxYkRDTmVHZzJQbmNMbFc2elRJaXBFQm9vaXhJRTlVN0t4SGdaeEJ5MEV0NkVFV3ZJVW1ucjZGNEYrZGJUCkQwNTBHSGxjWjdlT2VxWVRQWWVRQzUwMkcxRm80dGROaTRsRFA5TDlYWnBmN1ExUWltUkgycWFMUzAzWkZaYTIKdFk3YWgvUlFxWkw4RGt4eDgvemMyNXNnVEhWcHhvSzg1M2dsQlZCcy9FTk1peUdKV21BWFFheWV3WTNFUHQvOQp3R3dWNEttVTNkUERsZVFlWFNVR1BVSVNlUXhGankrakN3MjFwWXZpV1ZKVE5CQTlsNW55M0doRW1jbk9UL2dRCkhDdlZSTHlHTE1iYU1aNEpyUHdiK2FBdEJncmdlaUs0eGVTTU12cmJodz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
  private val arangoDB: ArangoDB = {
    val serde = JacksonSerde.of(com.arangodb.ContentType.JSON)
    //noinspection ConvertExpressionToSAM
    serde.configure(new Consumer[ObjectMapper] {
      override def accept(mapper: ObjectMapper): Unit = mapper
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
    })

    new ArangoDB.Builder()
      .useSsl(true)
      .verifyHost(false)
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
    "ssl.verifyHost" -> "false",
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
    val db = arangoDB.db(database)
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
