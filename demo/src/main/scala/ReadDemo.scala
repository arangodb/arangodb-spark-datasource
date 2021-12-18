import Schemas.movieSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ReadDemo {

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
    Demo.spark.read
      .format("com.arangodb.spark")
      .options(Demo.options + ("table" -> tableName))
      .schema(schema)
      .load
  }

  def readQuery(query: String, schema: StructType): DataFrame = {
    Demo.spark.read
      .format("com.arangodb.spark")
      .options(Demo.options + ("query" -> query))
      .schema(schema)
      .load
  }

}
