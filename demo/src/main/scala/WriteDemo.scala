import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object WriteDemo {

  val saveOptions: Map[String, String] = Demo.options ++ Map(
    "table.shards" -> "9",
    "confirmTruncate" -> "true",
    "overwriteMode" -> "replace"
  )

  def writeDemo(): Unit = {
    println("------------------")
    println("--- WRITE DEMO ---")
    println("------------------")

    println("Reading JSON files...")
    val nodesDF = Demo.spark.read.json(Demo.importPath + "/nodes.jsonl")
      .withColumn("releaseDate", unixTsToSparkDate(col("releaseDate")))
      .withColumn("birthday", unixTsToSparkDate(col("birthday")))
      .withColumn("lastModified", unixTsToSparkTs(col("lastModified")))
      .persist()
    val edgesDF = Demo.spark.read.json(Demo.importPath + "/edges.jsonl")
      .withColumn("_from", concat(lit("persons/"), col("_from")))
      .withColumn("_to", concat(lit("movies/"), col("_to")))
      .persist()

    val personsDF = dropNullColumns(nodesDF.where("type = 'Person'"))
    val moviesDF = dropNullColumns(nodesDF.where("type = 'Movie'"))
    val directedDF = dropNullColumns(edgesDF.where("`$label` = 'DIRECTED'"))
    val actedInDF = dropNullColumns(edgesDF.where("`$label` = 'ACTS_IN'"))

    println("Writing 'persons' collection...")
    saveDF(personsDF, "persons")

    println("Writing 'movies' collection...")
    saveDF(moviesDF, "movies")

    println("Writing 'directed' edge collection...")
    saveDF(directedDF, "directed", "edge")

    println("Writing 'actedIn' edge collection...")
    saveDF(actedInDF, "actedIn", "edge")
  }

  def unixTsToSparkTs(c: Column): Column = (c.cast(LongType) / 1000).cast(TimestampType)

  def unixTsToSparkDate(c: Column): Column = unixTsToSparkTs(c).cast(DateType)

  def dropNullColumns(df: DataFrame): DataFrame = df.drop(getEmptyColNames(df): _*)

  def saveDF(df: DataFrame, tableName: String, tableType: String = "document"): Unit =
    df
      .write
      .mode("overwrite")
      .format("com.arangodb.spark")
      .options(saveOptions ++ Map(
        "table" -> tableName,
        "table.type" -> tableType
      ))
      .save()

  def getEmptyColNames(df: DataFrame): Array[String] =
    df.columns.filter { colName =>
      df.filter(df(colName).isNotNull).count() == 0
    }
}
