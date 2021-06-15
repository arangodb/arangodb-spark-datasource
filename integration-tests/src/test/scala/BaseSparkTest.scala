import com.arangodb.entity.ServerRole
import com.arangodb.mapping.ArangoJack
import com.arangodb.{ArangoDB, ArangoDatabase}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.provider.Arguments

import java.util.stream

class BaseSparkTest {

  private val database = "sparkConnectorTest"
  private val user = "root"
  private val password = "test"
  private val endpoints = "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529"
  private val singleEndpoint = endpoints.split(',').head
  protected val arangoDB: ArangoDB = new ArangoDB.Builder()
    .user(user)
    .password(password)
    .host(singleEndpoint.split(':').head, singleEndpoint.split(':')(1).toInt)
    .serializer(new ArangoJack() {
      configure(new ArangoJack.ConfigureFunction {
        override def configure(mapper: ObjectMapper): Unit = mapper.registerModule(DefaultScalaModule)
      })
    })
    .build()
  protected val db: ArangoDatabase = arangoDB.db(database)
  private val isSingle: Boolean = arangoDB.getRole == ServerRole.SINGLE
  protected val options = Map(
    "database" -> database,
    "user" -> user,
    "password" -> password,
    "endpoints" -> {
      isSingle match {
        case true => singleEndpoint
        case false => endpoints
      }
    },
    "topology" -> {
      isSingle match {
        case true => "single"
        case false => "cluster"
      }
    }
  )

  protected val spark: SparkSession = SparkSession.builder()
    .appName("ArangoDBSparkTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()

  protected val usersDF: DataFrame = spark.read
    .format("org.apache.spark.sql.arangodb.datasource")
    .options(options + ("table" -> "users"))
    .schema(new StructType(
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
    ))
    .load()

  usersDF.createOrReplaceTempView("users")

  @AfterEach
  def shutdown(): Unit = {
    arangoDB.shutdown()
  }

}

object BaseSparkTest {
  def provideProtocolAndContentType(): stream.Stream[Arguments] = java.util.stream.Stream.of(
    Arguments.of("vst", "vpack"),
    Arguments.of("http", "vpack"),
    Arguments.of("http", "json")
  )
}
