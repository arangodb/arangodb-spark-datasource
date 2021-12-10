package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
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
        ArangoDBConf.COLLECTION -> "_fishbowl",
        ArangoDBConf.ENDPOINTS -> "172.17.0.1:8529",
        ArangoDBConf.ACQUIRE_HOST_LIST -> "true",
        ArangoDBConf.PASSWORD -> "test"
      ))
      .load()
      .show()
  }

}
