package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.junit.jupiter.api.{Disabled, Test}

@Disabled("manual test only")
class AcquireHostListTest {

  private val spark: SparkSession = SparkSession.builder()
    .appName("ArangoDBSparkTest")
    .master("local[*, 3]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()

  @Test
  def read(): Unit = {
    spark.read
      .format("org.apache.spark.sql.arangodb.datasource")
      .options(Map(
        ArangoOptions.COLLECTION -> "_fishbowl",
        ArangoOptions.ENDPOINTS -> "172.17.0.1:8529",
        ArangoOptions.ACQUIRE_HOST_LIST -> "true",
        ArangoOptions.PASSWORD -> "test"
      ))
      .load()
      .show()
  }

}
