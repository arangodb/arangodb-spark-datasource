import Schemas.movieSchema
import org.apache.spark.sql.SparkSession

object ReadWriteDemo {

  def readWriteDemo(): Unit = {
    println("-----------------------")
    println("--- READ-WRITE DEMO ---")
    println("-----------------------")

    println("Reading 'movies' collection and writing 'actionMovies' collection...")
    val actionMoviesDF = ReadDemo.readTable("movies", movieSchema)
      .select("_key", "title", "releaseDate", "runtime", "description")
      .filter("genre = 'Action'")
    WriteDemo.saveDF(actionMoviesDF, "actionMovies")
  }

}
