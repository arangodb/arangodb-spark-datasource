package examples

import org.apache.spark.sql.SparkSession

object MainCatalog extends App {

  val spark = SparkSession.builder()
    .appName("ArangoDB Spark Datasource")
    .config("spark.master", "local[*]")

    .config("spark.sql.catalog.arango1", "org.apache.spark.sql.arangodb.catalog.ArangoCatalog")
    .config("spark.sql.catalog.arango1.endpoints", "172.28.3.1:8529")
    .config("spark.sql.catalog.arango1.user", "root")
    .config("spark.sql.catalog.arango1.password", "test")

    .config("spark.sql.catalog.arango2", "org.apache.spark.sql.arangodb.catalog.ArangoCatalog")
    .config("spark.sql.catalog.arango2.endpoints", "172.28.3.2:8529")
    .config("spark.sql.catalog.arango2.user", "root")
    .config("spark.sql.catalog.arango2.password", "test")

    .getOrCreate()

  spark.sql(
    """
      |INSERT INTO arango2._system.users
      |     FROM arango1._system.users SELECT * WHERE gender == "female"
      |""".stripMargin)

}
