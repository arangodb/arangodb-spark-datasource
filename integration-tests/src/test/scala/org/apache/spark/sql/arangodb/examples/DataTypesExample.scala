package org.apache.spark.sql.arangodb.examples

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

object DataTypesExample {

  final case class Order(
                    userId: String,
                    price: Double,
                    shipped: Boolean,
                    totItems: Int,
                    creationDate: Date,
                    lastModifiedTs: Timestamp,
                    itemIds: List[String],
                    qty: Map[String, Int]
                  )

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ArangoDBSparkTest")
      .master("local[*, 3]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._

    val o = Order(
      userId = "Mike",
      price = 9.99,
      shipped = true,
      totItems = 2,
      creationDate = Date.valueOf(LocalDate.now()),
      lastModifiedTs = Timestamp.valueOf(LocalDateTime.now()),
      itemIds = List("itm1", "itm2"),
      qty = Map("itm1" -> 1, "itm2" -> 3)
    )

    val ds = Seq(o).toDS()

    ds.show()
    ds.printSchema()

    ds
      .write
      .mode("overwrite")
      .format("com.arangodb.spark")
      .option("password", "test")
      .option("endpoints", "172.17.0.1:8529")
      .option("table", "orders")
      .option("confirmTruncate", "true")
      .save()

    val readDS: Dataset[Order] = spark.read
      .format("com.arangodb.spark")
      .option("password", "test")
      .option("endpoints", "172.17.0.1:8529")
      .option("table", "orders")
      .schema(Encoders.product[Order].schema)
      .load()
      .as[Order]

    readDS.show()
    readDS.printSchema()

    assert(readDS.collect().head == o)
  }
}
