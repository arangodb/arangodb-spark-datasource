import time
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
        "integer": 1,
        "date": date.fromisoformat("2021-01-01").isoformat(),
        "timestampString": datetime.fromisoformat("2021-01-01 01:01:01.111").astimezone().isoformat(sep=" ", timespec="milliseconds"),
        "timestampMillis": datetime.fromisoformat("2021-01-01 01:01:01.111").astimezone().timestamp() * 1000,
        "string": "one",
        "intArray": [1, 1, 1],
        "struct": {
            "a": "a1",
            "b": 1
        }
    },
    {
        "bool": True,
        "integer": 2,
        "date": date.fromisoformat("2022-02-02").isoformat(),
        "timestampString": datetime.fromisoformat("2022-02-02 02:02:02.222").astimezone().isoformat(sep=" ", timespec="milliseconds"),
        "timestampMillis": datetime.fromisoformat("2022-02-02 02:02:02.222").astimezone().timestamp() * 1000,
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
    StructField("integer", IntegerType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("timestampString", TimestampType(), nullable=False),
    StructField("timestampMillis", TimestampType(), nullable=False),
    StructField("string", StringType(), nullable=False),
    StructField("intArray", ArrayType(IntegerType()), nullable=False),
    StructField("struct", StructType([
        StructField("a", StringType()),
        StructField("b", IntegerType())
    ]))
])


@pytest.fixture(scope="module")
def in_df(database_conn: arango.database.StandardDatabase, spark: SparkSession):
    df = test_basespark.create_df(database_conn, spark, "in", data, schema)
    yield df
    test_basespark.drop_table(database_conn, "in")


def test_bool(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    field_name = "bool"

    res = in_df.filter(col(field_name).isin(True, False)).collect()
    assert len(res) == 2

    sql_res = spark.sql(f"""
    SELECT * FROM in
    WHERE {field_name} IN (true, false)
    """).collect()
    assert len(sql_res) == 2


def test_integer(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    field_name = "integer"
    value = data[0][field_name]

    res = in_df.filter(col(field_name).isin(0, 1)).collect()
    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM in
        WHERE {field_name} IN (0, 1)
        """).collect()
    assert len(sql_res) == 1
    assert sql_res[0].asDict()[field_name] == value


def test_date(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    field_name = "date"
    value = data[0][field_name]
    value2 = date.fromisoformat("2020-01-01").isoformat()
    res = in_df.filter(col(field_name).isin(value, value2)).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == date.fromisoformat(value)

    sql_res = spark.sql(f"""
        SELECT * FROM in
        WHERE {field_name} IN ("{value}", "{value2}")
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == date.fromisoformat(value)


def test_timestamp(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    if spark.version.startswith("4"):
        pytest.xfail("FIXME")

    field_name = "timestampString"

    value = datetime.fromisoformat(data[0][field_name]).replace(tzinfo=None)
    value_str = value.isoformat(" ", timespec="milliseconds")
    value2 = datetime.fromisoformat("2020-01-01 00:00:00.000").isoformat(" ", timespec="milliseconds")

    res = in_df.filter(col(field_name).isin(value_str, value2)).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM in
        WHERE {field_name} IN ("{value_str}", "{value2}")
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value


def test_timestamp_millis(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    if spark.version.startswith("4"):
        pytest.xfail("FIXME")

    field_name = "timestampMillis"
    value = datetime.fromisoformat("2021-01-01 01:01:01.111")
    value_str = value.isoformat(" ", timespec="milliseconds")
    value2 = datetime.fromisoformat("2020-01-01 00:00:00.000")
    value2_str = value2.isoformat(" ", timespec="milliseconds")
    res = in_df.filter(col(field_name).isin(value_str, value2_str)).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name].timestamp() == value.timestamp()

    sql_res = spark.sql(f"""
        SELECT * FROM in
        WHERE {field_name} IN ("{value_str}", "{value2_str}")
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name].timestamp() == value.timestamp()


def test_string(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    field_name = "string"
    value = data[0][field_name]
    value2 = "foo"
    res = in_df.filter(col(field_name).isin(value, value2)).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
        SELECT * FROM in
        WHERE {field_name} IN ("{value}", "{value2}")
        """).collect()

    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value


def test_int_array(spark: SparkSession, in_df: pyspark.sql.DataFrame):
    field_name = "intArray"
    value = data[0][field_name]
    value2 = [4, 5, 6]
    res = in_df.filter(col(field_name).isin(array(*[lit(x) for x in value]), array(*[lit(x) for x in value2]))).collect()

    assert len(res) == 1
    assert res[0][field_name] == value

    sql_res = spark.sql(f"""
    SELECT * FROM in
    WHERE {field_name} IN (array({",".join([str(x) for x in value])}), array({",".join([str(x) for x in value2])}))
    """).collect()

    assert len(sql_res) == 1
    assert res[0][field_name] == value


def test_struct(spark: SparkSession):
    field_name = "struct"
    value = Row(a="a1", b=1)

    sql_res = spark.sql(f"""
    SELECT * FROM in
    WHERE {field_name} IN (("foo" AS a, 9 as b), ("a1" AS a, 1 AS b))
    """).collect()
    assert len(sql_res) == 1
    assert sql_res[0][field_name] == value