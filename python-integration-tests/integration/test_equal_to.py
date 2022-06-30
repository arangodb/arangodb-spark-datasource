from datetime import datetime, date

import arango.database
import pyspark.sql
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, BooleanType, DoubleType, FloatType, IntegerType, LongType, \
    DateType, TimestampType, ShortType, ByteType, StringType, ArrayType, Row

from integration import test_basespark

data = [
    {
        "bool": False,
        "double": 1.1,
        "float": 0.09375,
        "integer": 1,
        "long": 1,
        "date": date.fromisoformat("2021-01-01").isoformat(),
        "timestampString": datetime.fromisoformat("2021-01-01 01:01:01.111").isoformat(" "),
        "timestampMillis": datetime.fromisoformat("2021-01-01 01:01:01.111").timestamp(),
        "short": 1,
        "byte": 1,
        "string": "one",
        "intArray": [1, 1, 1],
        "struct": {
            "a": "a1",
            "b": 1
        }
    },
    {
        "bool": True,
        "double": 2.2,
        "float": 2.2,
        "integer": 2,
        "long": 2,
        "date": date.fromisoformat("2022-02-02").isoformat(),
        "timestampString": datetime.fromisoformat("2022-02-02 02:02:02.222").isoformat(" "),
        "timestampMillis": datetime.fromisoformat("2022-02-02 02:02:02.222").timestamp(),
        "short": 2,
        "byte": 2,
        "string": "two",
        "intArray": [2, 2, 2],
        "struct": {
            "a": "a2",
            "b": 2
        }
    }
]

schema = StructType([
    StructField("bool", BooleanType(), nullable=False),
    StructField("double", DoubleType(), nullable=False),
    StructField("float", FloatType(), nullable=False),
    StructField("integer", IntegerType(), nullable=False),
    StructField("long", LongType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("timestampString", TimestampType(), nullable=False),
    StructField("timestampMillis", TimestampType(), nullable=False),
    StructField("short", ShortType(), nullable=False),
    StructField("byte", ByteType(), nullable=False),
    StructField("string", StringType(), nullable=False),
    StructField("intArray", ArrayType(IntegerType()), nullable=False),
    StructField("struct", StructType([
        StructField("a", StringType()),
        StructField("b", IntegerType())
    ]))
])


@pytest.fixture(scope="module")
def eq_df(database_conn: arango.database.StandardDatabase, spark: SparkSession):
    df = test_basespark.create_df(database_conn, spark, "equalTo", data, schema)
    yield df
    test_basespark.drop_table(database_conn, "equalTo")


def test_bool(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("bool", spark, eq_df)


def test_double(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("double", spark, eq_df)


def test_float(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("float", spark, eq_df)


def test_integer(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("integer", spark, eq_df)


def test_long(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("long", spark, eq_df)


def test_date(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    field_name = "date"
    value = data[0][field_name]
    res = eq_df.filter(col(field_name) == value).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == date.fromisoformat(value)

    sql_res = spark.sql(f"""
        SELECT * FROM equalTo
        WHERE {field_name} = "{value}"
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == date.fromisoformat(value)


# FIXME: pyspark - python timestamp comparison
@pytest.mark.xfail
def test_timestamp_string(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    field_name = "timestampString"
    value = data[0][field_name]
    res = eq_df.filter(col(field_name) == to_timestamp(lit(value))).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM equalTo
        WHERE {field_name} = "{value}"
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value


# FIXME: pyspark - python timestamp comparison
@pytest.mark.xfail
def test_timestamp_millis(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    field_name = "timestampMillis"
    value = data[0][field_name]
    res = eq_df.filter(col(field_name) == to_timestamp(lit(value))).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM equalTo
        WHERE {field_name} = "{value}"
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value


def test_short(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("short", spark, eq_df)


def test_byte(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    verify_field_eq("byte", spark, eq_df)


def test_string(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    field_name = "string"
    value = data[0][field_name]
    res = eq_df.filter(col(field_name) == value).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM equalTo
        WHERE {field_name} = "{value}"
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value


def test_int_array(spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    field_name = "intArray"
    value = data[0][field_name]
    res = eq_df.filter(col(field_name) == array(*[lit(x) for x in value])).collect()

    assert len(res) == 1
    assert res[0][field_name] == value

    sql_res = spark.sql(f"""
    SELECT * FROM equalTo
    WHERE {field_name} = array({",".join([str(x) for x in value])})
    """).collect()

    assert len(sql_res) == 1
    assert res[0][field_name] == value


def test_struct(spark: SparkSession):
    field_name = "struct"
    value = Row(a="a1", b=1)

    sql_res = spark.sql(f"""
    SELECT * FROM equalTo
    WHERE {field_name} = struct("a1" AS a, 1 AS b)
    """).collect()
    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value


def verify_field_eq(field_name: str, spark: SparkSession, eq_df: pyspark.sql.DataFrame):
    value = data[0][field_name]
    res = eq_df.filter(col(field_name) == value).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM equalTo
        WHERE {field_name} = {value}
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value
