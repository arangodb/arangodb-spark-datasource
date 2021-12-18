import Schemas.movieSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Demo {
  val spark: SparkSession = SparkSession.builder
    .appName("arangodb-demo")
    .master("local[*, 3]")
    .getOrCreate

  val password = "test"
  val endpoints = "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549"

  val options = Map(
    "password" -> password,
    "endpoints" -> endpoints
  )

  val saveOptions: Map[String, String] = options ++ Map(
    "table.shards" -> "9",
    "confirmTruncate" -> "true",
    "overwriteMode" -> "replace"
  )

  def main(args: Array[String]): Unit = {
    writeDemo()
    readDemo()
    readWriteDemo()
    spark.stop
  }

  def writeDemo(): Unit = {
    println("------------------")
    println("--- WRITE DEMO ---")
    println("------------------")

    println("Reading JSON files...")
    val nodesDF = spark.read.json("docker/import/nodes.jsonl")
      .withColumn("releaseDate", unixTsToSparkDate(col("releaseDate")))
      .withColumn("birthday", unixTsToSparkDate(col("birthday")))
      .withColumn("lastModified", unixTsToSparkTs(col("lastModified")))
    val edgesDF = spark.read.json("docker/import/edges.jsonl")
      .withColumn("_from", concat(lit("persons/"), col("_from")))
      .withColumn("_to", concat(lit("movies/"), col("_to")))

    val personsDF = dropNullColumns(nodesDF.where("type = 'Person'")).persist()
    val moviesDF = dropNullColumns(nodesDF.where("type = 'Movie'")).persist()
    val directedDF = dropNullColumns(edgesDF.where("`$label` = 'DIRECTED'")).persist()
    val actedInDF = dropNullColumns(edgesDF.where("`$label` = 'ACTS_IN'")).persist()

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

  def readDemo(): Unit = {
    println("-----------------")
    println("--- READ DEMO ---")
    println("-----------------")

    val moviesDF = readTable("movies", movieSchema)

    println("Read table: history movies or documentaries about 'World War' released from 2000-01-01")
    moviesDF
      .select("title", "releaseDate", "genre", "description")
      .filter("genre IN ('History', 'Documentary') AND description LIKE '%World War%' AND releaseDate > '2000'")
      .show(20, 200)

    println("Read query: actors of movies directed by Clint Eastwood with related movie title and interpreted role")
    readQuery(
      """WITH movies, persons
        |FOR v, e, p IN 2 ANY "persons/1062" OUTBOUND directed, INBOUND actedIn
        |   RETURN {movie: p.vertices[1].title, name: v.name, role: p.edges[1].name}
        |""".stripMargin,
      schema = StructType(Array(
        StructField("movie", StringType),
        StructField("name", StringType),
        StructField("role", StringType)
      ))
    ).show(20, 200)
  }

  def readTable(tableName: String, schema: StructType): DataFrame = {
    spark.read
      .format("com.arangodb.spark")
      .options(options + ("table" -> tableName))
      .schema(schema)
      .load
  }

  def readQuery(query: String, schema: StructType): DataFrame = {
    spark.read
      .format("com.arangodb.spark")
      .options(options + ("query" -> query))
      .schema(schema)
      .load
  }

  def readWriteDemo(): Unit = {
    println("-----------------------")
    println("--- READ-WRITE DEMO ---")
    println("-----------------------")

    println("Reading 'movies' collection and writing 'actionMovies' collection...")
    val actionMoviesDF = readTable("movies", movieSchema)
      .select("_key", "title", "releaseDate", "runtime", "description")
      .filter("genre = 'Action'")
    saveDF(actionMoviesDF, "actionMovies")
  }

}
