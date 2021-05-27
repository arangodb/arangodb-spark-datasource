import org.apache.spark.sql.functions.{array_contains, col, lit}
import org.junit.jupiter.api.Test

class FilterTest extends BaseSparkTest {

  @Test
  def filterCollection(): Unit = {
    usersDF.printSchema()

    import spark.implicits._
    val usersDS = usersDF
      .filter(col("gender") === "female")
      .filter(col("birthday").lt(lit("1980-01-01")))
      .filter(col("name.first").startsWith("F"))
      .filter(col("name.last").isInCollection(Seq("Deely", "Bucher", "Natho")))
      .filter(array_contains(col("likes"), "running")) // not pushed down
      .as[User]

    usersDS.show()
  }

}
