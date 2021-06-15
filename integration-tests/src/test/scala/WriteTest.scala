import com.arangodb.ArangoCollection
import org.apache.spark.sql.SaveMode
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


class WriteTest extends BaseSparkTest {

  private val collection: ArangoCollection = db.collection("chessPlayers")

  @BeforeEach
  def init(): Unit = {
    if (!collection.exists()) {
      collection.create()
    }
    collection.truncate()
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def writeCollection(protocol: String, contentType: String): Unit = {
    import spark.implicits._
    val df = Seq(
      ("Carlsen", "Magnus"),
      ("Caruana", "Fabiano"),
      ("Ding", "Liren"),
      ("Nepomniachtchi", "Ian"),
      ("Aronian", "Levon"),
      ("Grischuk", "Alexander"),
      ("Giri", "Anish"),
      ("Mamedyarov", "Shakhriyar"),
      ("So", "Wesley"),
      ("Radjabov", "Teimour")
    ).toDF("surname", "name")
      .repartition(3)

    df.show()

    df.write
      .format("org.apache.spark.sql.arangodb.datasource")
      .mode(SaveMode.Append)
      .options(options + (
        "table" -> "chessPlayers",
        "protocol" -> protocol,
        "content-type" -> contentType
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
    val fromDb = db.query(
      """
        |FOR d IN chessPlayers FILTER d.surname == "Carlsen" RETURN d
        |""".stripMargin,
      classOf[Map[String, String]]).next()
    assertThat(fromDb).isNotNull
    assertThat(fromDb("name")).isEqualTo("Magnus")
    assertThat(fromDb("surname")).isEqualTo("Carlsen")
  }
}
