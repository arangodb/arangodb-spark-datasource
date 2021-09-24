package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class ReadTest extends BaseSparkTest {

  @Test
  def readCollection(): Unit = {
    import spark.implicits._
    val litalien = usersDF
      .filter(col("name.first") === "Prudence")
      .filter(col("name.last") === "Litalien")
      .filter(col("birthday") === "1944-06-19")
      .as[User]
      .first()

    assertThat(litalien.name.first).isEqualTo("Prudence")
    assertThat(litalien.name.last).isEqualTo("Litalien")
    assertThat(litalien.gender).isEqualTo("female")
    assertThat(litalien.likes).isEqualTo(Seq("swimming", "chess"))
    assertThat(litalien.birthday).isEqualTo("1944-06-19")
  }

  @Test
  def readCollectionSql(): Unit = {
    val litalien = spark.sql(
      """
        |SELECT likes
        |FROM users
        |WHERE name == ("Prudence" AS first, "Litalien" AS last)
        |    AND likes == ARRAY("swimming", "chess")
        |""".stripMargin)
      .first()
    assertThat(litalien.get(0)).isEqualTo(Seq("swimming", "chess"))
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def inferCollectionSchema(protocol: String, contentType: String): Unit = {
    val usersDF = spark.read
      .format("org.apache.spark.sql.arangodb.datasource")
      .options(options + (
        "table" -> "users",
        "protocol" -> protocol,
        "content-type" -> contentType
      ))
      .load()

    val nameSchema = usersDF.schema("name")
    assertThat(nameSchema).isInstanceOf(classOf[StructField])
    assertThat(nameSchema.name).isEqualTo("name")
    assertThat(nameSchema.dataType).isInstanceOf(classOf[StructType])
    assertThat(nameSchema.nullable).isTrue

    val firstNameSchema = nameSchema.dataType.asInstanceOf[StructType]("first")
    assertThat(firstNameSchema).isInstanceOf(classOf[StructField])
    assertThat(firstNameSchema.name).isEqualTo("first")
    assertThat(firstNameSchema.dataType).isInstanceOf(classOf[StringType])
    assertThat(firstNameSchema.nullable).isTrue

    val lastNameSchema = nameSchema.dataType.asInstanceOf[StructType]("last")
    assertThat(lastNameSchema).isInstanceOf(classOf[StructField])
    assertThat(lastNameSchema.name).isEqualTo("last")
    assertThat(lastNameSchema.dataType).isInstanceOf(classOf[StringType])
    assertThat(lastNameSchema.nullable).isTrue
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def readQuery(protocol: String, contentType: String): Unit = {
    // directors of Kevin Bacon's movies
    val query =
      """
        |FOR v,e,p IN 2..2 ANY "persons/759"
        |    GRAPH imdb
        |    FILTER IS_SAME_COLLECTION("actsIn", p.edges[0]) AND
        |        IS_SAME_COLLECTION("directed", p.edges[1])
        |    RETURN v
        |""".stripMargin.replaceAll("\n", "")

    val directorsDF = spark.read
      .format("org.apache.spark.sql.arangodb.datasource")
      .options(options + (
        "query" -> query,
        "protocol" -> protocol,
        "content-type" -> contentType
      ))
      .load()

    assertThat(directorsDF.count()).isEqualTo(46)
  }

}
