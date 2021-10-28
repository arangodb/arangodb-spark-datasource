package org.apache.spark.sql.arangodb.commons.filter

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class InFilterTest {
  private val schema = StructType(Array(
    // atomic types
    StructField("bool", BooleanType),
    StructField("double", DoubleType),
    StructField("float", FloatType),
    StructField("integer", IntegerType),
    StructField("long", LongType),
    StructField("date", DateType),
    StructField("timestamp", TimestampType),
    StructField("short", ShortType),
    StructField("byte", ByteType),
    StructField("string", StringType),

    // complex types
    StructField("array", ArrayType(StringType)),
    StructField("null", NullType),
    StructField("struct", StructType(Array(
      StructField("a", StringType),
      StructField("b", IntegerType)
    )))
  ))

  @Test
  def inStringFilter(): Unit = {
    val field = "string"
    val values: Array[Any] = Array("a", "b", "c")
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    val escapedValues = values.map(v => s""""$v"""")
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${escapedValues.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inFilterTimestamp(): Unit = {
    val field = "timestamp"
    val values: Array[Any] = Array(
      "2001-01-02T15:30:45.678111Z",
      "2001-01-02T15:30:45.678111Z",
      "2001-01-02T15:30:45.678111Z"
    )
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.PARTIAL)
    val quotedValues = values.map(v=>s""""$v"""")
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${quotedValues.mkString(",")}][* FILTER DATE_TIMESTAMP(`d`.`$field`) == DATE_TIMESTAMP(CURRENT)]) > 0""")
  }

  @Test
  def inFilterDate(): Unit = {
    val field = "date"
    val values: Array[Any] = Array(
      "2001-01-02",
      "2001-01-02",
      "2001-01-02"
    )
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    val quotedValues = values.map(v=>s""""$v"""")
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${quotedValues.mkString(",")}][* FILTER DATE_TIMESTAMP(`d`.`$field`) == DATE_TIMESTAMP(CURRENT)]) > 0""")
  }

  @Test
  def inBoolFilter(): Unit = {
    val field = "bool"
    val values: Array[Any] = Array(false, true, false)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inDoubleFilter(): Unit = {
    val field = "double"
    val values: Array[Any] = Array(77.88, 77.88, 77.88)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inFloatFilter(): Unit = {
    val field = "float"
    val values: Array[Any] = Array(77.88F, 77.88F, 77.88F)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inIntegerFilter(): Unit = {
    val field = "integer"
    val values: Array[Any] = Array(22, 33, 44)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inLongFilter(): Unit = {
    val field = "long"
    val values: Array[Any] = Array(22L, 33L, 44L)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inShortFilter(): Unit = {
    val field = "short"
    val values: Array[Any] = Array(22.toShort, 33.toShort, 44.toShort)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inByteFilter(): Unit = {
    val field = "byte"
    val values: Array[Any] = Array(22.toByte, 33.toByte, 44.toByte)
    val filter = PushableFilter(In(field, values), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inArrayFilter(): Unit = {
    val field = "array"
    val values: Array[Seq[String]] = Array(
      Seq("a", "b", "c"),
      Seq("a", "b", "c"),
      Seq("a", "b", "c")
    )
    val filter = PushableFilter(In(field, values.asInstanceOf[Array[Any]]), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([${values.map(v => v.map(x => "\"" + x + "\"").mkString("[", ",", "]")).mkString(",")}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

  @Test
  def inStructFilter(): Unit = {
    val field = "struct"
    val values: Array[GenericRowWithSchema] = Array(
      new GenericRowWithSchema(
        Array("str", 22),
        StructType(Array(
          StructField("a", StringType),
          StructField("b", IntegerType)
        ))
      ),
      new GenericRowWithSchema(
        Array("str", 22),
        StructType(Array(
          StructField("a", StringType),
          StructField("b", IntegerType)
        ))
      )
    )
    val filter = PushableFilter(In(field, values.asInstanceOf[Array[Any]]), schema: StructType)
    assertThat(filter.support()).isEqualTo(FilterSupport.FULL)
    assertThat(filter.aql("d"))
      .isEqualTo(s"""LENGTH([{"a":"str","b":22},{"a":"str","b":22}][* FILTER `d`.`$field` == CURRENT]) > 0""")
  }

}
