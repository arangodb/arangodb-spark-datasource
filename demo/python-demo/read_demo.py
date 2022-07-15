from typing import Dict

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from schemas import movie_schema
from utils import combine_dicts


def read_demo(spark: SparkSession, base_opts: Dict[str, str]):
    movies_df = read_collection(spark, "movies", base_opts, movie_schema)

    print("Read table: history movies or documentaries about 'World War' released from 2000-01-01")
    # We can get to what we want in 2 different ways:
    # First, the PySpark dataframe way...
    movies_df \
        .select("title", "releaseDate", "genre", "description") \
        .filter("genre IN ('History', 'Documentary') AND description LIKE '%World War%' AND releaseDate > '2000'") \
        .show()

    # Second, in the Pandas on Spark way...
    movies_pd_df = movies_df.to_pandas_on_spark()
    subset = movies_pd_df[["title", "releaseDate", "genre", "description"]]
    recent_ww_movies = subset[subset["genre"].isin(["History", "Documentary"])\
                              & (subset["releaseDate"] >= '2000')\
                              & subset["description"].str.contains("World War")]
    print(recent_ww_movies)

    print("Read query: actors of movies directed by Clint Eastwood with related movie title and interpreted role")
    read_aql_query(
        spark,
        """WITH movies, persons
          FOR v, e, p IN 2 ANY "persons/1062" OUTBOUND directed, INBOUND actedIn
             RETURN {movie: p.vertices[1].title, name: v.name, role: p.edges[1].name}
        """,
        base_opts,
        StructType([
            StructField("movie", StringType()),
            StructField("name", StringType()),
            StructField("role", StringType())
        ])
    ).show(20, 200)


def read_collection(spark: SparkSession, collection_name: str, base_opts: Dict[str, str], schema: StructType) -> pyspark.sql.DataFrame:
    arangodb_datasource_options = combine_dicts([base_opts, {"table": collection_name}])

    return spark.read \
        .format("com.arangodb.spark") \
        .options(**arangodb_datasource_options) \
        .schema(schema) \
        .load()


def read_aql_query(spark: SparkSession, query: str, base_opts: Dict[str, str], schema: StructType) -> pyspark.sql.DataFrame:
    arangodb_datasource_options = combine_dicts([base_opts, {"query": query}])

    return spark.read \
        .format("com.arangodb.spark") \
        .options(**arangodb_datasource_options) \
        .schema(schema) \
        .load()
