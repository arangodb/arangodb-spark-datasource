from datetime import date
from typing import Any, List, Dict

import arango.database
from arango import ArangoClient
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.types import StructType, ArrayType, DateType, StringType, StructField
import pytest

from integration.utils import combine_dicts

database = "sparkConnectorTest"
user = "sparkUser"
password = "sparkTest"
root_user = "root"
root_password = "test"
is_single = None
options = {
    "database": database,
    "user": user,
    "password": password,
}
arango_datasource_name = "com.arangodb.spark"

users_schema = StructType([
    StructField("likes", ArrayType(StringType(), containsNull=False)),
    StructField("birthday", DateType(), nullable=True),
    StructField("gender", StringType(), nullable=False),
    StructField("name", StructType(
        [StructField("first", StringType(), nullable=True), StructField("last", StringType(), nullable=False)]
    ), nullable=True)
])

protocol_and_content_type = [
    ("http", "vpack"),
    ("http", "json"),
    ("http2", "vpack"),
    ("http2", "json")
]


@pytest.fixture(scope="session")
def adb_hostname(pytestconfig):
    return pytestconfig.getoption("adb_hostname")


@pytest.fixture(scope="session")
def endpoints(adb_hostname: str):
    return f"{adb_hostname}:8529,{adb_hostname}:8539,{adb_hostname}:8549"


@pytest.fixture(scope="session")
def single_endpoint(endpoints):
    return endpoints.split(",")[0]


@pytest.fixture(scope="session")
def arangodb_client(single_endpoint: str):
    arangodb = ArangoClient(f"http://{single_endpoint}")
    yield arangodb
    arangodb.close()


@pytest.fixture(scope="session")
def database_conn(arangodb_client, single_endpoint, endpoints):
    db = init_db(arangodb_client)
    is_single = db.cluster.server_role() == "SINGLE"

    if is_single:
        options["endpoints"] = single_endpoint
    else:
        options["endpoints"] = endpoints

    yield db


@pytest.fixture(scope="session")
def spark(database_conn, pytestconfig):
    spark_session = SparkSession.builder \
        .appName("ArangoDBPySparkTest") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.jars", pytestconfig.getoption("datasource_jar_loc")) \
        .getOrCreate()
    setup_users_df(database_conn, spark_session)
    yield spark_session
    spark_session.stop()


def setup_users_df(db, spark_session) -> None:
    users_df = create_df(db, spark_session, "users", [
        {
            "name": {"first": "Prudence", "last": "Litalien"},
            "gender": "female",
            "birthday": "1944-06-19",
            "likes": ["swimming", "chess"]
        },
        {
            "name": {"first": "Ernie", "last": "Levinson"},
            "gender": "male",
            "birthday": "1955-07-25",
            "likes": []
        },
        {
            "name": {"first": "Malinda", "last": "Siemon"},
            "gender": "female",
            "birthday": "1993-04-10",
            "likes": ["climbing"]
        },
    ], users_schema)
    users_df.show()


def create_df(db: arango.database.StandardDatabase, spark_session: SparkSession, name: str, docs: List[Any], schema: StructType, additional_options: Dict[str, str] = None) -> pyspark.sql.DataFrame:
    if additional_options is None:
        additional_options = {}

    if db.has_collection(name):
        col = db.collection(name)
        col.truncate()
    else:
        col = db.create_collection(name, shard_count=6)

    col.insert_many(docs)
    all_opts = combine_dicts([options, additional_options, {"table": name}])

    df_reader = spark_session.read \
        .format(arango_datasource_name) \
        .options(**all_opts) \

    if schema:
        df_reader = df_reader.schema(schema)

    df = df_reader.load()
    df.createOrReplaceTempView(name)
    return df


def create_query_df(spark_session: SparkSession, query: str, schema: StructType, additional_options: Dict[str, str] = None) -> pyspark.sql.DataFrame:
    if additional_options is None:
        additional_options = {}

    all_opts = {}
    for d in [options, additional_options, {"query": query}]:
        all_opts.update(d)

    df_reader = spark_session.read \
        .format(arango_datasource_name) \
        .options(**all_opts) \

    if schema:
        df_reader = df_reader.schema(schema)

    return df_reader.load()


def drop_table(database_conn, name: str) -> None:
    database_conn.delete_collection(name)


def init_db(arangodb: ArangoClient) -> arango.database.StandardDatabase:
    sys_db = arangodb.db("_system", username=root_user, password=root_password)
    if not sys_db.has_user(user):
        sys_db.create_user(user, password, active=True)

    if not sys_db.has_database(database):
        sys_db.create_database(database)
    sys_db.update_permission(user, "rw", database)

    return arangodb.db(database, username=user, password=password)