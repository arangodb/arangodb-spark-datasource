import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("arangodb-demo")
      .getOrCreate()

    val options = Map(
      "database" -> "sparkConnectorTest",
      "user" -> "root",
      "password" -> "test",
      "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529"
    )

    val usersDF: DataFrame = spark.read
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

    usersDF.filter(col("birthday") === "1982-12-15").show()

    spark.stop()
  }
}
