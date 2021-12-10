import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("arangodb-demo")
      .master("local[*, 3]")
      .getOrCreate()

    val options = Map(
      "user" -> "root",
      "password" -> "test",
      "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549"
    )

    val schema = new StructType(
      Array(
        StructField("likes", ArrayType(StringType)),
        StructField("birthday", DateType),
        StructField("gender", StringType),
        StructField("name", StructType(
          Array(
            StructField("first", StringType),
            StructField("last", StringType)
          )
        )),
        StructField("contact", StructType(
          Array(
            StructField("address", StructType(
              Array(
                StructField("city", StringType),
                StructField("state", StringType),
                StructField("street", StringType),
                StructField("zip", StringType)
              )
            ))
          )
        ))
      )
    )

    val usersDF = spark.read
      .format("com.arangodb.spark")
      .options(options + ("table" -> "users"))
      .schema(schema)
      .load()
    usersDF.show()
    usersDF.printSchema()
    usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()

    // Spark SQL
    usersDF.createOrReplaceTempView("users")
    val californians = spark.sql("SELECT * FROM users WHERE contact.address.state = 'CA'")
    californians.show()
    californians.write.format("com.arangodb.spark").mode(org.apache.spark.sql.SaveMode.Overwrite).options(options + ("table" -> "californians", "confirm.truncate" -> "true")).save()

    spark.stop()
  }
}
