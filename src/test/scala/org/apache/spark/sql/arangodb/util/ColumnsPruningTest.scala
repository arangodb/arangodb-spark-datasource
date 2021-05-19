package org.apache.spark.sql.arangodb.util

import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ColumnsPruningTest {

  @Test
  def generateAqlReturnClause(): Unit = {
    val schema = StructType(Array(
      StructField(""""birthday"""", DateType),
      StructField("gender", StringType),
      StructField("likes", ArrayType(StringType)),
      StructField("name", StructType(Array(
        StructField("first", StringType),
        StructField("last", StringType),
      )))
    ))

    val res = ArangoClient.generateColumnsFilter(schema, "d")
    assertThat(res).isEqualTo(
      """
        |{
        |  `"birthday"`: `d`.`"birthday"`,
        |  `gender`: `d`.`gender`,
        |  `likes`: `d`.`likes`,
        |  `name`: {
        |    `first`: `d`.`name`.`first`,
        |    `last`: `d`.`name`.`last`
        |  }
        |}
        |""".stripMargin.replaceAll("\\s", ""))
  }

}
