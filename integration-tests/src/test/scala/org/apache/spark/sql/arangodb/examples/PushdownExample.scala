package org.apache.spark.sql.arangodb.examples

import com.arangodb.ArangoDB
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object PushdownExample {

  final case class User(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    prepareDB()

    val spark: SparkSession = SparkSession.builder()
      .appName("ArangoDBSparkTest")
      .master("local[*, 3]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._
    val ds: Dataset[User] = spark.read
      .format("com.arangodb.spark")
      .option("password", "test")
      .option("endpoints", "172.28.0.1:8529")
      .option("table", "users")
      .schema(Encoders.product[User].schema)
      .load()
      .as[User]

    ds
      .select("name")
      .filter("age >= 18 AND age < 22")
      .show

    /*
    Generated query:
       FOR d IN @@col FILTER `d`.`age` >= 18 AND `d`.`age` < 22 RETURN {`name`:`d`.`name`}
       with params: Map(@col -> users)
     */
  }

  private def prepareDB(): Unit = {
    val arangoDB = new ArangoDB.Builder()
      .host("172.28.0.1", 8529)
      .password("test")
      .build()

    val col = arangoDB.db().collection("users")
    if (!col.exists())
      col.create()
    col.truncate()
    col.insertDocument(User("Alice", 10))
    col.insertDocument(User("Bob", 20))
    col.insertDocument(User("Eve", 30))

    arangoDB.shutdown()
  }

}
