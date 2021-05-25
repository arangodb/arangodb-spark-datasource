package examples

import com.arangodb.ArangoDB
import org.apache.spark.sql.{SaveMode, SparkSession}

object MainReadWrite extends App {

  val arangoDB = new ArangoDB.Builder().build()

  val collectionName = "persons_copy"
  val collection = arangoDB.db().collection(collectionName)
  if (!collection.exists()) {
    collection.create()
  }
  collection.truncate()

  val spark = SparkSession.builder()
    .appName("ArangoDB Spark Datasource")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val personsDF = spark.read
    .format("org.apache.spark.sql.arangodb.datasource")
    .options(Map(
      "table" -> "persons",
      "database" -> "_system",
      "user" -> "root",
      "password" -> "test",
      "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529"
    ))
    .load()

  personsDF.show()
  personsDF.printSchema()

  personsDF
    .write
    .format("org.apache.spark.sql.arangodb.datasource")
    .mode(SaveMode.Append)
    .options(Map(
      "database" -> "_system",
      "table" -> collectionName,
      "user" -> "root",
      "password" -> "test",
      "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529"
    ))
    .save()

  println(s"count: ${personsDF.count()}")
  println(s"count: ${collection.count().getCount}")

  arangoDB.shutdown()

}
