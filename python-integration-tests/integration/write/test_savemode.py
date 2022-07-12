import arango
import arango.database
import arango.collection
import pytest
import pyspark.sql
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from integration.test_basespark import protocol_and_content_type, options, arango_datasource_name
from integration.utils import combine_dicts


COLLECTION_NAME = "chessPlayersSaveMode"

data = [
    ("Carlsen", "Magnus"),
    ("Caruana", "Fabiano"),
    ("Ding", "Liren"),
    ("Nepomniachtchi", "Ian"),
    ("Aronian", "Levon"),
    ("Grischuk", "Alexander"),
    ("Giri", "Anish"),
    ("Mamedyarov", "Shakhriyar"),
    ("So", "Wesley"),
    ("Radjabov", "Teimour")
]


@pytest.fixture(scope="function")
def chess_collection(database_conn: arango.database.StandardDatabase) -> arango.collection.StandardCollection:
    if database_conn.has_collection(COLLECTION_NAME):
        database_conn.delete_collection(COLLECTION_NAME)
    yield database_conn.collection(COLLECTION_NAME)


@pytest.fixture
def chess_df(spark: SparkSession) -> pyspark.sql.DataFrame:
    df = spark.createDataFrame(data, schema=["surname", "name"])
    return df


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_savemode_append(chess_df: pyspark.sql.DataFrame, chess_collection: arango.collection.StandardCollection, protocol: str, content_type: str):
    all_opts = combine_dicts([options, {
        "table": COLLECTION_NAME,
        "protocol": protocol,
        "contentType": content_type
    }])

    chess_df.write\
        .format(arango_datasource_name)\
        .mode("Append")\
        .options(**all_opts)\
        .save()

    assert chess_collection.count() == 10


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_savemode_append_with_existing_collection(chess_df: pyspark.sql.DataFrame, chess_collection: arango.collection.StandardCollection, database_conn: arango.database.StandardDatabase, protocol: str, content_type: str):
    database_conn.create_collection(COLLECTION_NAME)
    chess_collection.insert({})

    all_opts = combine_dicts([options, {
        "table": COLLECTION_NAME,
        "protocol": protocol,
        "contentType": content_type
    }])

    chess_df.write \
        .format(arango_datasource_name) \
        .mode("Append") \
        .options(**all_opts) \
        .save()

    assert chess_collection.count() == 11


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_savemode_overwrite_should_throw_whenused_alone(chess_df: pyspark.sql.DataFrame, chess_collection: arango.collection.StandardCollection, protocol: str, content_type: str):
    all_opts = combine_dicts([options, {
        "table": COLLECTION_NAME,
        "protocol": protocol,
        "contentType": content_type
    }])

    with pytest.raises(AnalysisException) as e:
        chess_df.write \
            .format(arango_datasource_name) \
            .mode("Overwrite") \
            .options(**all_opts) \
            .save()

    e.match("confirmTruncate")


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_savemode_overwrite(chess_df: pyspark.sql.DataFrame, chess_collection: arango.collection.StandardCollection, protocol: str, content_type: str):
    all_opts = combine_dicts([options, {
        "table": COLLECTION_NAME,
        "protocol": protocol,
        "contentType": content_type,
        "confirmTruncate": "true"
    }])

    chess_df.write \
        .format(arango_datasource_name) \
        .mode("Overwrite") \
        .options(**all_opts) \
        .save()

    assert chess_collection.count() == 10


@pytest.mark.parametrize("protocol,content_type", protocol_and_content_type)
def test_savemode_overwrite_with_existing_collection(chess_df: pyspark.sql.DataFrame, chess_collection: arango.collection.StandardCollection, database_conn: arango.database.StandardDatabase, protocol: str, content_type: str):
    database_conn.create_collection(COLLECTION_NAME)
    chess_collection.insert({})

    all_opts = combine_dicts([options, {
        "table": COLLECTION_NAME,
        "protocol": protocol,
        "contentType": content_type,
        "confirmTruncate": "true"
    }])

    chess_df.write \
        .format(arango_datasource_name) \
        .mode("Overwrite") \
        .options(**all_opts) \
        .save()

    assert chess_collection.count() == 10
