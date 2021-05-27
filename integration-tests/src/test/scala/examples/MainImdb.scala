package examples

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Michele Rastelli
 */
object MainImdb extends App {
  val spark = SparkSession.builder()
    .appName("IMDB Spark")
    .config("spark.master", "local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val nodes = spark.read
    .json("./docker/import/imdb/nodes.json")

  val edges = spark.read
    .json("./docker/import/imdb/edges.json")
    .withColumn("_from", concat(lit("persons/"), col("_from")))
    .withColumn("_to", concat(lit("movies/"), col("_to")))

  def getEmptyColNames(df: DataFrame): Seq[String] = {
    df.columns.filter { colName =>
      df.filter(df(colName).isNotNull).count() == 0
    }
  }

  def dropEmptyCols(df: DataFrame): DataFrame = {
    df.drop(getEmptyColNames(df): _*)
  }

  val dfs = Map(
    "persons" -> nodes.where(col("type") === "Person").drop(col("type")),
    "movies" -> nodes.where(col("type") === "Movie").drop(col("type")),
    "directed" -> edges.where(col("$label") === "DIRECTED").drop(col("$label")),
    "actsIn" -> edges.where(col("$label") === "ACTS_IN").drop(col("$label"))
  )
    .mapValues(dropEmptyCols)
    .mapValues(_.distinct())

  dfs.foreach(df => df._2.write.json("./docker/import/imdb/output/" + df._1))
}
