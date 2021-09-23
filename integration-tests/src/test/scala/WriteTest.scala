import com.arangodb.ArangoCollection
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


class WriteTest extends BaseSparkTest {

  private val collection: ArangoCollection = db.collection("chessPlayers")

  private val df = {
    import spark.implicits._
    Seq(
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
  }

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

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwriteAloneShouldThrow(protocol: String, contentType: String): Unit = {
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.write
        .format("org.apache.spark.sql.arangodb.datasource")
        .mode(SaveMode.Overwrite)
        .options(options + (
          "table" -> "chessPlayers",
          "protocol" -> protocol,
          "content-type" -> contentType
        ))
        .save()
    })

    assertThat(thrown).isInstanceOf(classOf[IllegalArgumentException])
    assertThat(thrown.getMessage).contains("""please set "confirm.truncate"""")
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def saveModeOverwrite(protocol: String, contentType: String): Unit = {
    collection.insertDocument(new Object)

    df.write
      .format("org.apache.spark.sql.arangodb.datasource")
      .mode(SaveMode.Overwrite)
      .options(options + (
        "table" -> "chessPlayers",
        "protocol" -> protocol,
        "content-type" -> contentType,
        ArangoOptions.CONFIRM_TRUNCATE -> "true"
      ))
      .save()

    assertThat(collection.count().getCount).isEqualTo(10L)
  }


}
