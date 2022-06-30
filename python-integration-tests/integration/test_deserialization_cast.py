from typing import Dict, List, Any

import arango.database
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

from integration import test_basespark

COLLECTION_NAME = "deserializationCast"
content_types = ["vpack", "json"]


def check_implicit_cast(db: arango.database.StandardDatabase, spark: SparkSession, schema: StructType, data: List[Dict[str, Any]], json_data: List[str], content_type: str):
    # FIXME: many vpack tests are failing
    if content_type == "vpack":
        pytest.xfail("Too many vpack tests fail")

    df_from_json = spark.read.schema(schema).json(spark.sparkContext.parallelize(json_data))
    df_from_json.show()

    df = test_basespark.create_df(db, spark, COLLECTION_NAME, data, schema, {"contentType": content_type})
    assert df.collect() == df_from_json.collect()


@pytest.mark.parametrize("content_type", content_types)
def test_number_int_to_string_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", StringType())]),
        [{"a": 1}],
        ['{"a":1}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_number_dec_to_string_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", StringType())]),
        [{"a": 1.1}],
        ['{"a":1.1}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_bool_to_string_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", StringType())]),
        [{"a": True}],
        ['{"a":true}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_object_to_string_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", StringType())]),
        [{"a": {"b": "c"}}],
        ['{"a":{"b":"c"}}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_array_to_string_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", StringType())]),
        [{"a": [1, 2]}],
        ['{"a":[1,2]}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_null_to_integer_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", IntegerType(), nullable=False)]),
        [{"a": None}],
        ['{"a":null}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_null_to_double_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", DoubleType(), nullable=False)]),
        [{"a": None}],
        ['{"a":null}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_null_to_boolean_cast(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_implicit_cast(
        database_conn,
        spark,
        StructType([StructField("a", BooleanType(), nullable=False)]),
        [{"a": None}],
        ['{"a":null}'],
        content_type
    )
