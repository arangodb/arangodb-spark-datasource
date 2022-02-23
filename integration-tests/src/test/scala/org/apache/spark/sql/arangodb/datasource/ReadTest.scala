package org.apache.spark.sql.arangodb.datasource

import com.arangodb.ArangoDBException
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{NullType, NumericType, StringType, StructField, StructType}
import org.assertj.core.api.Assertions.{assertThat, catchThrowable}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{MethodSource, ValueSource}

import java.util.concurrent.TimeoutException

class ReadTest extends BaseSparkTest {

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def readCollection(protocol: String, contentType: String): Unit = {
    val df = spark.read
      .format(BaseSparkTest.arangoDatasource)
      .options(options + (
        ArangoDBConf.COLLECTION -> "users",
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType
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
        ArangoDBConf.COLLECTION -> "users",
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType
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
  @ValueSource(strings = Array("vpack", "json"))
  def inferCollectionSchemaWithCorruptRecordColumn(contentType: String): Unit = {
    assumeTrue(isSingle)

    val additionalOptions = Map(
      ArangoDBConf.COLUMN_NAME_OF_CORRUPT_RECORD -> "badRecord",
      ArangoDBConf.SAMPLE_SIZE -> "2",
      ArangoDBConf.CONTENT_TYPE -> contentType
    )

    doInferCollectionSchemaWithCorruptRecordColumn(
      BaseSparkTest.createQueryDF(
        """FOR d IN [{"v":1},{"v":2},{"v":"3"}] RETURN d""",
        schema = null,
        additionalOptions
      )
    )

    doInferCollectionSchemaWithCorruptRecordColumn(
      BaseSparkTest.createDF(
        "badData",
        Seq(
          Map("v" -> 1),
          Map("v" -> 2),
          Map("v" -> "3")
        ),
        schema = null,
        additionalOptions
      )
    )
  }

  def doInferCollectionSchemaWithCorruptRecordColumn(df: DataFrame): Unit = {
    val vSchema = df.schema("v")
    assertThat(vSchema).isInstanceOf(classOf[StructField])
    assertThat(vSchema.name).isEqualTo("v")
    assertThat(vSchema.dataType).isInstanceOf(classOf[NumericType])
    assertThat(vSchema.nullable).isTrue

    val badRecordSchema = df.schema("badRecord")
    assertThat(badRecordSchema).isInstanceOf(classOf[StructField])
    assertThat(badRecordSchema.name).isEqualTo("badRecord")
    assertThat(badRecordSchema.dataType).isInstanceOf(classOf[StringType])
    assertThat(badRecordSchema.nullable).isTrue

    val badRecords = df.filter("badRecord IS NOT NULL").persist()
      .select("badRecord")
      .collect()
      .map(_ (0).asInstanceOf[String])

    assertThat(badRecords).hasSize(1)
    assertThat(badRecords.head).contains(""""v":"3""")
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
        ArangoDBConf.QUERY -> query,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType
      ))
      .load()

    assertThat(df.count()).isEqualTo(10)
  }

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def reatTimeout(protocol: String, contentType: String): Unit = {
    val query =
      """
        |RETURN { value: SLEEP(5) }
        |""".stripMargin.replaceAll("\n", "")

    val df = spark.read
      .format(BaseSparkTest.arangoDatasource)
      .schema(StructType(Array(
        StructField("value", NullType)
      )))
      .options(options + (
        ArangoDBConf.QUERY -> query,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType,
        ArangoDBConf.TIMEOUT -> "1000"
      ))
      .load()

    val thrown: Throwable = catchThrowable(new ThrowingCallable() {
      override def call(): Unit = df.show()
    })

    assertThat(thrown)
      .isInstanceOf(classOf[SparkException])

    assertThat(thrown.getCause)
      .isInstanceOf(classOf[ArangoDBException])

    assertThat(thrown.getCause.getCause)
      .isInstanceOf(classOf[TimeoutException])
  }

}
