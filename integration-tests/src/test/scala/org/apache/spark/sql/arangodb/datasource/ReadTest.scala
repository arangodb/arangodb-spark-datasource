package org.apache.spark.sql.arangodb.datasource

import org.apache.spark.SparkException
import org.apache.spark.sql.arangodb.commons.ArangoOptions
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class ReadTest extends BaseSparkTest {

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def readCollection(protocol: String, contentType: String): Unit = {
    val df = spark.read
      .format(BaseSparkTest.arangoDatasource)
      .options(options + (
        ArangoOptions.COLLECTION -> "users",
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .schema(BaseSparkTest.usersSchema)
      .load()


    import spark.implicits._
    val litalien = df
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

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def readCollectionWithBadRecords(protocol: String, contentType: String): Unit = {
    val thrown = catchThrowable(new ThrowingCallable() {
      override def call(): Unit =
        spark.read
          .format(BaseSparkTest.arangoDatasource)
          .options(options + (
            ArangoOptions.COLLECTION -> "users",
            ArangoOptions.PROTOCOL -> protocol,
            ArangoOptions.CONTENT_TYPE -> contentType
          ))
          .schema(new StructType(
            Array(
              StructField("likes", IntegerType)
            )
          ))
          .load()
          .show()
    })

    assertThat(thrown).isInstanceOf(classOf[SparkException])
    assertThat(thrown.getCause).isInstanceOf(classOf[BadRecordException])
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
      .format(BaseSparkTest.arangoDatasource)
      .options(options + (
        ArangoOptions.COLLECTION -> "users",
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
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
    val query =
      """
        |FOR i IN 1..10
        | RETURN { idx: i, value: SHA1(i) }
        |""".stripMargin.replaceAll("\n", "")

    val df = spark.read
      .format(BaseSparkTest.arangoDatasource)
      .options(options + (
        ArangoOptions.QUERY -> query,
        ArangoOptions.PROTOCOL -> protocol,
        ArangoOptions.CONTENT_TYPE -> contentType
      ))
      .load()

    assertThat(df.count()).isEqualTo(10)
  }

}
