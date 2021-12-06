package org.apache.spark.sql.arangodb.datasource

import com.arangodb.entity.ServerRole
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.CollectionCreateOptions
import com.arangodb.{ArangoDB, ArangoDatabase}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeAll}
import org.junit.jupiter.params.provider.Arguments

import java.sql.Date
import java.time.LocalDate
import java.util
import java.util.stream
import scala.collection.JavaConverters.asJavaIterableConverter

class BaseSparkTest {

  protected val arangoDB: ArangoDB = BaseSparkTest.arangoDB
  protected val db: ArangoDatabase = BaseSparkTest.db

  protected val spark: SparkSession = BaseSparkTest.spark
  protected val options: Map[String, String] = BaseSparkTest.options
  protected val usersDF: DataFrame = BaseSparkTest.usersDF

  @AfterEach
  def shutdown(): Unit = {
    arangoDB.shutdown()
  }

  def isSingle: Boolean = BaseSparkTest.isSingle

  def isCluster: Boolean = !BaseSparkTest.isSingle
}

object BaseSparkTest {

  def provideProtocolAndContentType(): stream.Stream[Arguments] = java.util.stream.Stream.of(
    Arguments.of("vst", "vpack"),
    Arguments.of("http", "vpack"),
    Arguments.of("http", "json")
  )

  val arangoDatasource = "org.apache.spark.sql.arangodb.datasource"
  private val database = "sparkConnectorTest"
  private val user = "root"
  private val password = "test"
  val endpoints = "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549"
  private val singleEndpoint = endpoints.split(',').head
  private val arangoDB: ArangoDB = new ArangoDB.Builder()
    .user(user)
    .password(password)
    .host(singleEndpoint.split(':').head, singleEndpoint.split(':')(1).toInt)
    .serializer(new ArangoJack() {
      //noinspection ConvertExpressionToSAM
      configure(new ArangoJack.ConfigureFunction {
        override def configure(mapper: ObjectMapper): Unit = mapper
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
    })
    .build()
  private val db: ArangoDatabase = arangoDB.db(database)
  private val isSingle: Boolean = arangoDB.getRole == ServerRole.SINGLE
  private val options = Map(
    "database" -> database,
    "user" -> user,
    "password" -> password,
    "endpoints" -> {
      if (isSingle) {
        singleEndpoint
      } else {
        endpoints
      }
    },
    "topology" -> {
      if (isSingle) {
        "single"
      } else {
        "cluster"
      }
    }
  )

  private val spark: SparkSession = SparkSession.builder()
    .appName("ArangoDBSparkTest")
    .master("local[*, 3]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()

  val usersSchema = new StructType(
    Array(
      StructField("likes", ArrayType(StringType, containsNull = false)),
      StructField("birthday", DateType, nullable = true),
      StructField("gender", StringType, nullable = false),
      StructField("name", StructType(
        Array(
          StructField("first", StringType, nullable = true),
          StructField("last", StringType, nullable = false)
        )
      ), nullable = true)
    )
  )

  private lazy val usersDF: DataFrame = createDF("users",
    Seq(
      Map(
        "name" -> Map(
          "first" -> "Prudence",
          "last" -> "Litalien"
        ),
        "gender" -> "female",
        "birthday" -> "1944-06-19",
        "likes" -> Seq(
          "swimming",
          "chess"
        )
      ),
      Map(
        "name" -> Map(
          "first" -> "Ernie",
          "last" -> "Levinson"
        ),
        "gender" -> "male",
        "birthday" -> "1955-07-25",
        "likes" -> Seq()
      ),
      Map(
        "name" -> Map(
          "first" -> "Malinda",
          "last" -> "Siemon"
        ),
        "gender" -> "female",
        "birthday" -> "1993-04-10",
        "likes" -> Seq(
          "climbing"
        )
      )
    ),
    usersSchema
  )

  def createDF(name: String, docs: Iterable[Any], schema: StructType, additionalOptions: Map[String, String] = Map.empty): DataFrame = {
    val col = db.collection(name)
    if (col.exists()) {
      col.truncate()
    } else {
      db.createCollection(name, new CollectionCreateOptions().numberOfShards(6))
    }
    col.insertDocuments(docs.asJava.asInstanceOf[util.Collection[Any]])

    val df = spark.read
      .format(arangoDatasource)
      .options(options ++ additionalOptions + (ArangoOptions.COLLECTION -> name))
      .schema(schema)
      .load()
    df.createOrReplaceTempView(name)
    df
  }

  def createQueryDF(query: String, schema: StructType, additionalOptions: Map[String, String] = Map.empty): DataFrame =
    spark.read
      .format(arangoDatasource)
      .options(options ++ additionalOptions + (ArangoOptions.QUERY -> query))
      .schema(schema)
      .load()

  def dropTable(name: String): Unit = {
    db.collection(name).drop()
  }

  @BeforeAll
  def beforeAll(): Unit = {
    if (!db.exists())
      db.create()
  }
}
