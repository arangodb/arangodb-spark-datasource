import com.arangodb.ArangoDB
import org.apache.spark.sql.arangodb.datasource.ArangoOptions
import org.apache.spark.sql.arangodb.util.ArangoClient
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.provider.Arguments

import java.util.stream

class BaseSparkTest {

  protected val options = Map(
    "db" -> "sparkConnectorTest",
    "user" -> "root",
    "password" -> "test",
    "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529"
  )

  protected var client: ArangoClient = _
  protected var arangoDB: ArangoDB = _

  protected val spark: SparkSession = SparkSession.builder()
    .appName("ArangoDBSparkTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()

  protected val usersDF: DataFrame = spark.read
    .format("org.apache.spark.sql.arangodb.datasource")
    .options(options + ("collection" -> "users"))
    .schema(new StructType(
      Array(
        StructField("likes", ArrayType(StringType, containsNull = false)),
        StructField("birthday", DateType, nullable = true),
        StructField("gender", StringType, nullable = false),
        StructField("name", StructType(
          Array(
            StructField("first", StringType, nullable = true),
            StructField("last", StringType, nullable = false),
          )
        ), nullable = true)
      )
    ))
    .load()

  @BeforeEach
  def startup(): Unit = {
    client = ArangoClient(ArangoOptions(options))
    arangoDB = client.arangoDB
  }

  @AfterEach
  def shutdown(): Unit = {
    arangoDB.shutdown()
  }

}

object BaseSparkTest {
  def provideProtocolAndContentType(): stream.Stream[Arguments] = java.util.stream.Stream.of(
    Arguments.of("VST", "vpack"),
    Arguments.of("HTTP", "vpack"),
    Arguments.of("HTTP", "json")
  )
}
