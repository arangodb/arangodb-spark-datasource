import ast
import datetime

import arango
import pyspark.sql
import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, NumericType, NullType

from integration.test_basespark import options, users_schema, arango_datasource_name, protocol_and_content_type, \
    create_query_df, create_df
from integration.utils import combine_dicts


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_read_collection(spark: SparkSession, protocol: str, content_type: str):
    test_options = {
        "table": "users",
        "protocol": protocol,
        "contentType": content_type
    }
    all_opts = combine_dicts([options, test_options])

    df = spark.read\
        .format(arango_datasource_name)\
        .options(**all_opts)\
        .schema(users_schema)\
        .load()

    litalien = df\
        .filter(df.name.first == "Prudence")\
        .filter(df.name.last == "Litalien") \
        .filter(df.birthday == "1944-06-19")\
        .first().asDict()

    assert litalien["name"]["first"] == "Prudence"
    assert litalien["name"]["last"] == "Litalien"
    assert litalien["gender"] == "female"
    assert litalien["likes"] == ["swimming", "chess"]
    assert litalien["birthday"] == datetime.date(1944, 6, 19)


def test_read_collection_sql(spark: SparkSession):
    litalien = spark.sql("""
    SELECT likes
    FROM users
    WHERE name == ("Prudence" AS first, "Litalien" AS last)
    AND likes == ARRAY("swimming", "chess") 
    """).first().asDict()

    assert litalien["likes"] == ["swimming", "chess"]


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_infer_collection_schema(spark: SparkSession, protocol: str, content_type: str):
    test_options = {
        "table": "users",
        "protocol": protocol,
        "contentType": content_type
    }
    all_opts = combine_dicts([options, test_options])

    df = spark.read \
        .format(arango_datasource_name) \
        .options(**all_opts) \
        .schema(users_schema) \
        .load()

    name_schema = df.schema.__getitem__("name")
    assert isinstance(name_schema, StructField)
    assert name_schema.name == "name"
    assert isinstance(name_schema.dataType, StructType)
    assert name_schema.nullable

    first_name_schema = name_schema.dataType.__getitem__("first")
    assert isinstance(first_name_schema, StructField)
    assert first_name_schema.name == "first"
    assert isinstance(first_name_schema.dataType, StringType)
    assert first_name_schema.nullable

    last_name_schema = name_schema.dataType.__getitem__("last")
    assert isinstance(last_name_schema, StructField)
    assert last_name_schema.name == "last"
    assert isinstance(last_name_schema.dataType, StringType)
    assert not last_name_schema.nullable


@pytest.mark.parametrize("content_type", ["vpack", "json"])
def test_infer_collection_schema_with_corrupt_record_column(database_conn: arango.database.StandardDatabase, spark: SparkSession, content_type: str):
    if database_conn.cluster.server_role() != "SINGLE":
        pytest.xfail("Inference order only guaranteed when not in Cluster deployment")

    additional_options = {
        "columnNameOfCorruptRecord": "badRecord",
        "sampleSize": "2",
        "contentType": content_type
    }
    do_infer_collection_schema_with_corrupt_record_column(
        create_query_df(
            spark_session=spark,
            query="""FOR d in [{"v":1}, {"v":2}, {"v":"3"}] RETURN d""",
            schema=None,
            additional_options=additional_options
        )
    )

    do_infer_collection_schema_with_corrupt_record_column(
        create_df(
            database_conn, spark, "badData", [{"v": 1}, {"v": 2}, {"v": "3"}], schema=None, additional_options=additional_options
        )
    )


def do_infer_collection_schema_with_corrupt_record_column(df: pyspark.sql.DataFrame):
    v_schema = df.schema.__getitem__("v")
    assert isinstance(v_schema, StructField)
    assert v_schema.name == "v"
    assert isinstance(v_schema.dataType, NumericType)
    assert v_schema.nullable

    bad_record_schema = df.schema.__getitem__("badRecord")
    assert isinstance(bad_record_schema, StructField)
    assert bad_record_schema.name == "badRecord"
    assert isinstance(bad_record_schema.dataType, StringType)
    assert bad_record_schema.nullable

    bad_records = [x.asDict() for x in df.filter("badRecord IS NOT NULL").persist()\
        .select("badRecord")\
        .collect()]

    assert len(bad_records) == 1
    assert "v" in bad_records[0]["badRecord"]
    assert ast.literal_eval(bad_records[0]["badRecord"])["v"] == "3"


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_read_query(spark: SparkSession, protocol: str, content_type: str):
    query = """FOR i in 1..10 RETURN { idx: i, value: SHA1(i) }"""
    test_options = {
        "query": query,
        "protocol": protocol,
        "contentType": content_type
    }
    all_opts = combine_dicts([options, test_options])

    df = spark.read\
        .format(arango_datasource_name)\
        .options(**all_opts)\
        .load()

    assert df.count() == 10


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_read_timeout(spark: SparkSession, protocol: str, content_type: str):
    query = "RETURN { value: SLEEP(5) }"
    test_options = {
        "query": query,
        "protocol": protocol,
        "contentType": content_type,
        "timeout": "1000"
    }
    all_opts = combine_dicts([options, test_options])

    df = spark.read \
        .format(arango_datasource_name) \
        .schema(StructType([StructField("value", NullType())]))\
        .options(**all_opts)\
        .load()

    with pytest.raises(Py4JJavaError) as e:
        df.show()

    e.match("SparkException")
    e.match("ArangoDBException")
