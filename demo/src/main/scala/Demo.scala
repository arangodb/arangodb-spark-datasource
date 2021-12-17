import Schemas.{actsInSchema, directedSchema, movieSchema, personSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType, TimestampType}
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
    spark.stop
  }

  def writeDemo(): Unit = {
    val nodesDF = spark.read.json("docker/import/nodes.jsonl")
      .withColumn("releaseDate", unixTsToSparkDate(col("releaseDate")))
      .withColumn("birthday", unixTsToSparkDate(col("birthday")))
      .withColumn("lastModified", unixTsToSparkTs(col("lastModified")))
    val edgesDF = spark.read.json("docker/import/edges.jsonl")
      .withColumn("_from", concat(lit("persons/"), col("_from")))
      .withColumn("_to", concat(lit("movies/"), col("_to")))

    saveDF(
      nodesDF.where("type = 'Person'"),
      tableName = "persons",
      tableType = "document"
    )

    saveDF(
      nodesDF.where("type = 'Movie'"),
      tableName = "movies",
      tableType = "document"
    )

    saveDF(
      df = edgesDF.where("`$label` = 'DIRECTED'"),
      tableName = "directed",
      tableType = "edge"
    )

    saveDF(
      df = edgesDF.where("`$label` = 'ACTS_IN'"),
      tableName = "actedIn",
      tableType = "edge"
    )
  }

  def unixTsToSparkTs(c: Column): Column = (c.cast(LongType) / 1000).cast(TimestampType)

  def unixTsToSparkDate(c: Column): Column = unixTsToSparkTs(c).cast(DateType)

  def saveDF(df: DataFrame, tableName: String, tableType: String): Unit =
    df
      .drop(getEmptyColNames(df): _*)
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
    val moviesDF = readTable("movies", movieSchema)
    val personsDF = readTable("persons", personSchema)
    val actedInDF = readTable("actedIn", actsInSchema)
    val directedDF = readTable("directed", directedSchema)

    // History movies or Documentaries about World War released from 2000-01-01
    moviesDF
      .select("title", "releaseDate", "genre", "description")
      .filter("releaseDate > '2000'")
      .filter("genre IN ('History', 'Documentary')")
      .filter("description LIKE '%World War%'")
      .show(20, 200)

    // actors in Titanic with roles
    moviesDF
      .join(actedInDF, moviesDF("_id") === actedInDF("_to"))
      .join(personsDF, actedInDF("_from") === personsDF("_id"))
      .filter(moviesDF("_id") === "movies/6002")
      .select(personsDF("name"), actedInDF("name").as("role"))
      .show(100, 100)

    // actors in Titanic with roles (AQL query)
    readQuery(
      """WITH persons
        |  FOR v, e, p IN 1 INBOUND "movies/6002" actedIn
        |  RETURN {name: v.name, role: e.name}
        |""".stripMargin, schema = StructType(Array(
        StructField("name", StringType),
        StructField("role", StringType)
      ))
    ).show(200, 100)

    // actors of movies directed by Clint Eastwood with related movie title
    readQuery(
      """WITH movies, persons
        |FOR v, e, p IN 2 ANY "persons/1062" directed, actedIn
        |   FILTER IS_SAME_COLLECTION(directed, p.edges[0])
        |   RETURN {name: v.name, movie: p.vertices[1].title}
        |""".stripMargin,
      schema = StructType(Array(
        StructField("name", StringType),
        StructField("movie", StringType)
      ))
    ).show(200, 100)
  }

  def readTable(tableName: String, schema: StructType): DataFrame = {
    spark.read
      .format("com.arangodb.spark")
      .options(options ++ Map(
        "table" -> tableName,
        "mode" -> "FAILFAST" // fail on bad records
      ))
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

}
