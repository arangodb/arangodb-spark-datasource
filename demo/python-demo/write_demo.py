import datetime
import pathlib
from typing import Dict

from pyspark import pandas as ps
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType

from utils import combine_dicts
from schemas import person_schema, movie_schema, directed_schema, acts_in_schema


def save_df(ps_df, table_name: str, options: Dict[str, str], table_type: str = None) -> None:
    if not table_type:
        table_type = "document"

    all_opts = combine_dicts([options, {
        "table.shards": "9",
        "confirmTruncate": "true",
        "overwriteMode": "replace",
        "table": table_name,
        "table.type": table_type
    }])

    ps_df.to_spark()\
        .write\
        .mode("overwrite")\
        .format("com.arangodb.spark")\
        .options(**all_opts)\
        .save()


def write_demo(spark: SparkSession, save_opts: Dict[str, str], import_path_str: str):
    import_path = pathlib.Path(import_path_str)

    print("Read Nodes from JSONL using Pandas on Spark API")
    nodes_pd_df = ps.read_json(str(import_path / "nodes.jsonl"))
    nodes_pd_df = nodes_pd_df[nodes_pd_df["_key"].notnull()]
    nodes_pd_df["releaseDate"] = ps.to_datetime(nodes_pd_df["releaseDate"], unit="ms")
    nodes_pd_df["birthday"] = ps.to_datetime(nodes_pd_df["birthday"], unit="ms")

    def convert_to_timestamp(to_modify, column):
        tz_aware_datetime = datetime.datetime.utcfromtimestamp(
                               int(to_modify[column])/1000
                           ).replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
        tz_naive = tz_aware_datetime.replace(tzinfo=None)
        to_modify[column] = tz_naive
        return to_modify

    nodes_pd_df = nodes_pd_df.apply(convert_to_timestamp, axis=1, args=("lastModified",))

    nodes_df = nodes_pd_df.to_spark()
    nodes_pd_df = nodes_df\
        .withColumn("releaseDate", f.to_date(nodes_df["releaseDate"])) \
        .withColumn("birthday", f.to_date(nodes_df["birthday"])) \
        .to_pandas_on_spark()

    print("Read Edges from JSONL using PySpark API")
    edges_df = spark.read.json(str(import_path / "edges.jsonl"))
    # apply the schema to change nullability of _key, _from, and _to columns in schema
    edges_pd_df = edges_df.to_pandas_on_spark()
    edges_pd_df["_from"] = "persons/" + edges_pd_df["_from"]
    edges_pd_df["_to"] = "movies/" + edges_pd_df["_to"]

    print("Create the collection dfs")
    persons_df = nodes_pd_df[nodes_pd_df["type"] == "Person"][person_schema.fieldNames()[1:]]
    movies_df = nodes_pd_df[nodes_pd_df["type"] == "Movie"][movie_schema.fieldNames()[1:]]
    directed_df = edges_pd_df[edges_pd_df["$label"] == "DIRECTED"][directed_schema.fieldNames()[1:]]
    acted_in_df = edges_pd_df[edges_pd_df["$label"] == "ACTS_IN"][acts_in_schema.fieldNames()[1:]]

    # _from and _to need to be set with nullable=False in the schema in order for it to work
    directed_df = spark.createDataFrame(directed_df.to_spark().rdd, StructType(
        directed_schema.fields[1:])).to_pandas_on_spark()
    acted_in_df = spark.createDataFrame(acted_in_df.to_spark().rdd, StructType(
        acts_in_schema.fields[1:])).to_pandas_on_spark()

    print("writing the persons collection")
    save_df(persons_df, "persons", save_opts)
    print("writing the movies collection")
    save_df(movies_df, "movies", save_opts)
    print("writing the 'directed' edge collection")
    save_df(directed_df, "directed", save_opts, "edge")
    print("writing the 'actedIn' collection")
    save_df(acted_in_df, "actedIn", save_opts, "edge")
