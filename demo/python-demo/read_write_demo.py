from typing import Dict

from pyspark.sql import SparkSession

import read_demo
import write_demo
from schemas import movie_schema


def read_write_demo(spark: SparkSession, opts: Dict[str, str]):
    print("-----------------------")
    print("--- READ-WRITE DEMO ---")
    print("-----------------------")

    print("Reading 'movies' collection and writing 'actionMovies' collection...")
    action_movies_df = read_demo.read_collection(spark, "movies", opts, movie_schema)\
        .select("_key", "title", "releaseDate", "runtime", "description")\
        .filter("genre = 'Action'")
    write_demo.save_df(action_movies_df.to_pandas_on_spark(), "actionMovies", opts)
    print("You can now view the actionMovies collection in ArangoDB!")