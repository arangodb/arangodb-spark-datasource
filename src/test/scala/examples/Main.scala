package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import java.sql.Date

object Main extends App {

  val spark = SparkSession.builder()
    .appName("ArangoDB Spark Datasource")
    .config("spark.master", "local[*]")
    .config("spark.sql.catalog.arango", "org.apache.spark.sql.arangodb.catalog.ArangoCatalog")
    .getOrCreate()

  case class User(
                   firstName: String,
                   birthday: Date
                 )

  import spark.implicits._

  val userDS = spark.sql(
    """
      |SELECT
      |   name.first AS firstName,
      |   to_date(birthday, "yyyy-MM-dd") as birthday
      |FROM arango._system.users
      |""".stripMargin
  ).as[User]
  userDS.take(10).foreach(u => println(u))

  //  FAIL: org.apache.spark.sql.AnalysisException: org.apache.spark.sql.arangodb.datasource is not a valid Spark SQL Data Source.
  //  SEE: https://issues.apache.org/jira/browse/SPARK-25280
  //  spark.sql("CREATE TABLE spark_catalog.default.tfv (birthday DATE) USING org.apache.spark.sql.arangodb.datasource")
  //  spark.sql("SELECT * FROM tfv").show()

  //  OK
  spark.sql("SELECT * FROM arango._system.users").show()

  //  OK
  spark.sql("CREATE VIEW zzz AS SELECT * FROM arango._system.users")
  spark.sql("SELECT * FROM zzz").show()

  //  OK
  //  spark.sql("CREATE TABLE rr USING PARQUET AS SELECT * FROM arango._system.users")
  //  spark.sql("SELECT * FROM rr").show()

  val usersDF = spark.read
    .format("org.apache.spark.sql.arangodb.datasource")
    .options(Map(
      "db" -> "sparkConnectorTest",
      "user" -> "root",
      "password" -> "test",
      "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529",
      "collection" -> "users"
    ))
    .schema(new StructType(
      Array(
        StructField("likes", ArrayType(StringType)),
        StructField("birthday", DateType),
        StructField("gender", StringType),
        StructField("name", StructType(
          Array(
            StructField("first", StringType),
            StructField("last", StringType),
          )
        ))
      )
    ))
    .load()

  usersDF
    .createOrReplaceTempView("sqlUsers")

  val oldAndYoungs = spark.sql(
    """
      |SELECT name.first AS firstName, birthday, likes FROM sqlUsers
      |WHERE
      |   birthday < CAST('1941-01-01' AS DATE) OR
      |   birthday > CAST('1993-01-01' AS DATE)
      |""".stripMargin)

  oldAndYoungs.show()

  println(oldAndYoungs.rdd.getNumPartitions)
  println(oldAndYoungs.count())

  val startingWithA = oldAndYoungs
    .select(col("firstName"))
    .where(col("firstName").startsWith("A"))

  startingWithA.explain()
  startingWithA.show()

}
