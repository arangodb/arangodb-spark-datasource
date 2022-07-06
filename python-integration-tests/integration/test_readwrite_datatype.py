import copy
import sys
from datetime import datetime, date
from decimal import Context

import arango.database
import pyspark.sql
import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType, DoubleType, FloatType, IntegerType, LongType, \
    DateType, TimestampType, ShortType, ByteType, StringType, ArrayType, Row, MapType, DecimalType

from integration import test_basespark
from integration.utils import combine_dicts

COLLECTION_NAME = "datatypes"

data = [
    [
        False,
        1.1,
        0.09375,
        1,
        1,
        date.fromisoformat("2021-01-01"),
        datetime.fromisoformat("2021-01-01 01:01:01.111").astimezone(),
        1,
        1,
        "one",
        [1, 1, 1],
        [["a", "b", "c"], ["d", "e", "f"]],
        {"a": 1, "b": 1},
        Row("a1", 1)
    ],
    [
        True,
        2.2,
        2.2,
        2,
        2,
        date.fromisoformat("2022-02-02"),
        datetime.fromisoformat("2022-02-02 02:02:02.222").astimezone(),
        2,
        2,
        "two",
        [2, 2, 2],
        [["a", "b", "c"], ["d", "e", "f"]],
        {"a": 2, "b": 2},
        Row("a1", 2)
    ]
]

struct_fields = [
    StructField("bool", BooleanType(), nullable=False),
    StructField("double", DoubleType(), nullable=False),
    StructField("float", FloatType(), nullable=False),
    StructField("integer", IntegerType(), nullable=False),
    StructField("long", LongType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("short", ShortType(), nullable=False),
    StructField("byte", ByteType(), nullable=False),
    StructField("string", StringType(), nullable=False),
    StructField("intArray", ArrayType(IntegerType()), nullable=False),
    StructField("stringArrayArray", ArrayType(ArrayType(StringType())), nullable=False),
    StructField("intMap", MapType(StringType(), IntegerType()), nullable=False),
    StructField("struct", StructType([
        StructField("a", StringType()),
        StructField("b", IntegerType())
    ]))
]

schema = StructType(struct_fields)


def test_write_decimal_type_with_json_content_should_throw(spark: SparkSession):
    schema_with_decimal = StructType(copy.deepcopy(struct_fields))
    schema_with_decimal.add(StructField("decimal", DecimalType(38, 18), nullable=False))
    df = spark.createDataFrame(spark.sparkContext.parallelize([Row(*x, Context(prec=38).create_decimal("1.111111111")) for x in data]), schema_with_decimal)

    with pytest.raises(Py4JJavaError) as e:
        write_df(df, "http", "json")

    e.match("UnsupportedOperationException")
    e.match("Cannot write DecimalType when using contentType=json")


@pytest.mark.parametrize("protocol,content_type", test_basespark.protocol_and_content_type)
def test_round_trip_read_write(spark: SparkSession, protocol: str, content_type: str):
    df = spark.createDataFrame(spark.sparkContext.parallelize([Row(*x) for x in data]), schema)
    round_trip_readwrite(spark, df, protocol, content_type)


@pytest.mark.parametrize("protocol,content_type", test_basespark.protocol_and_content_type)
def test_round_trip_read_write_decimal_type(spark: SparkSession, protocol: str, content_type: str):
    if content_type != "vpack" or spark.version.startswith("2.4"):
        pytest.xfail("vpack and Spark 2.4 don't support round trip decimal types")

    schema_with_decimal = StructType(copy.deepcopy(struct_fields))
    schema_with_decimal.add(StructField("decimal", DecimalType(38, 18), nullable=False))
    df = spark.createDataFrame(spark.sparkContext.parallelize([Row(*x, Context(prec=38).create_decimal("2.22222222")) for x in data]), schema_with_decimal)
    round_trip_readwrite(spark, df, protocol, content_type)


def write_df(df: pyspark.sql.DataFrame, protocol: str, content_type: str):
    all_opts = combine_dicts([
        test_basespark.options,
        {
            "table": COLLECTION_NAME,
            "protocol": protocol,
            "contentType": content_type,
            "overwriteMode": "replace",
            "confirmTruncate": "true"
        }
    ])
    df.write\
        .format(test_basespark.arango_datasource_name)\
        .mode("overwrite")\
        .options(**all_opts)\
        .save()


def round_trip_readwrite(spark: SparkSession, df: pyspark.sql.DataFrame, protocol: str, content_type: str):
    initial = df.collect()

    all_opts = combine_dicts([test_basespark.options, {"table": COLLECTION_NAME}])

    write_df(df, protocol, content_type)
    read = spark.read.format(test_basespark.arango_datasource_name)\
        .options(**all_opts)\
        .schema(df.schema)\
        .load()\
        .collect()

    assert initial.sort() == read.sort()
