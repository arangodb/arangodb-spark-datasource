from typing import Any, Dict, List

import arango.database
import pytest
from pyspark.errors import SparkRuntimeException
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType, BooleanType

from integration import test_basespark


content_types = ["vpack", "json"]
COLLECTION_NAME = "deserializationCast"


def do_test_bad_record(db: arango.database.StandardDatabase, spark: SparkSession, schema: StructType, data: List[Dict[str, Any]], json_data: List[str], opts: Dict[str, str]):
    df_from_json = spark.read.schema(schema).options(**opts).json(spark.sparkContext.parallelize(json_data))
    df_from_json.show()

    table_df = test_basespark.create_df(db, spark, COLLECTION_NAME, data, schema, opts)
    assert table_df.collect() == df_from_json.collect()

    query_df = test_basespark.create_query_df(spark, f"RETURN {json_data[0]}", schema, opts)
    assert query_df.collect() == df_from_json.collect()


def check_bad_record(db: arango.database.StandardDatabase, spark: SparkSession, schema: StructType, data: List[Dict[str, Any]], json_data: List[str], content_type: str):
    # Permissive
    do_test_bad_record(db, spark, schema, data, json_data, {"contentType": content_type})

    # Permissive with column name of corrupt record
    do_test_bad_record(
        db,
        spark,
        schema.add(StructField("corruptRecord", StringType())),
        data,
        json_data,
        {
            "contentType": content_type,
            "columnNameOfCorruptRecord": "corruptRecord"
        }
    )

    # Dropmalformed
    do_test_bad_record(db, spark, schema, data, json_data,
                       {
                           "contentType": content_type,
                           "mode": "DROPMALFORMED"
                       })

    # Failfast
    df = test_basespark.create_df(db, spark, COLLECTION_NAME, data, schema,
                                  {
                                      "contentType": content_type,
                                      "mode": "FAILFAST"
                                  })
    with pytest.raises((Py4JJavaError, SparkRuntimeException)) as e:
        df.collect()

    e.match("Malformed record")
    if spark.version.startswith("3"):
        e.match("BadRecordException")


@pytest.mark.parametrize("content_type", content_types)
def test_string_as_integer(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_bad_record(
        database_conn,
        spark,
        StructType([StructField("a", IntegerType())]),
        [{"a": "1"}],
        ['{"a":"1"}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_boolean_as_integer(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_bad_record(
        database_conn,
        spark,
        StructType([StructField("a", IntegerType())]),
        [{"a": True}],
        ['{"a":true}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_string_as_double(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_bad_record(
        database_conn,
        spark,
        StructType([StructField("a", DoubleType())]),
        [{"a": "1"}],
        ['{"a":"1"}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_boolean_as_double(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_bad_record(
        database_conn,
        spark,
        StructType([StructField("a", DoubleType())]),
        [{"a": True}],
        ['{"a":true}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_string_as_boolean(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_bad_record(
        database_conn,
        spark,
        StructType([StructField("a", BooleanType())]),
        [{"a": "true"}],
        ['{"a":"true"}'],
        content_type
    )


@pytest.mark.parametrize("content_type", content_types)
def test_number_as_boolean(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    check_bad_record(
        database_conn,
        spark,
        StructType([StructField("a", BooleanType())]),
        [{"a": 1}],
        ['{"a":1}'],
        content_type
    )