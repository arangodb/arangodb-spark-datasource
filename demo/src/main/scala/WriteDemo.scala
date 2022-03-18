import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
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
      .withColumn("_from", new Column(AssertNotNull(concat(lit("persons/"), col("_from")).expr)))
      .withColumn("_to", new Column(AssertNotNull(concat(lit("movies/"), col("_to")).expr)))
      .persist()

    val personsDF = nodesDF
      .select(Schemas.personSchema.fieldNames.filter(_ != "_id").map(col): _*)
      .where("type = 'Person'")
    val moviesDF = nodesDF
      .select(Schemas.movieSchema.fieldNames.filter(_ != "_id").map(col): _*)
      .where("type = 'Movie'")
    val directedDF = edgesDF
      .select(Schemas.directedSchema.fieldNames.filter(_ != "_id").map(col): _*)
      .where("`$label` = 'DIRECTED'")
    val actedInDF = edgesDF
      .select(Schemas.actsInSchema.fieldNames.filter(_ != "_id").map(col): _*)
      .where("`$label` = 'ACTS_IN'")

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

}
