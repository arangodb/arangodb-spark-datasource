import os
import pathlib
from argparse import ArgumentParser
from typing import Dict

from pyspark.sql import SparkSession

from read_write_demo import read_write_demo
from read_demo import read_demo
from write_demo import write_demo


def create_spark_session() -> SparkSession:
    # Here we can initialize the spark session, and in doing so,
    # include the ArangoDB Spark DataSource package
    arango_spark_version = os.environ["ARANGO_SPARK_VERSION"]

    spark = SparkSession.builder \
        .appName("ArangoDBPySparkDataTypesExample") \
        .master("local[*]") \
        .config("spark.jars.packages", f"com.arangodb:arangodb-spark-datasource-3.5_2.12:{arango_spark_version}") \
        .getOrCreate()

    return spark


def create_base_arangodb_datasource_opts(password: str, endpoints: str, ssl_enabled: str, ssl_cert_value: str) -> Dict[str, str]:
    return {
        "password": password,
        "endpoints": endpoints,
        "ssl.enabled": ssl_enabled,
        "ssl.cert.value": ssl_cert_value,
    }


def main():
    parser = ArgumentParser()
    parser.add_argument("--import-path", default=None)
    parser.add_argument("--password", default="test")
    parser.add_argument("--endpoints", default="localhost:8529")
    parser.add_argument("--ssl-enabled", default="false")
    parser.add_argument("--ssl-cert-value", default="")
    args = parser.parse_args()

    if args.import_path is None:
        args.import_path = pathlib.Path(__file__).resolve().parent.parent / "docker" / "import"

    spark = create_spark_session()
    base_opts = create_base_arangodb_datasource_opts(args.password, args.endpoints, args.ssl_enabled, args.ssl_cert_value)
    write_demo(spark, base_opts, args.import_path)
    read_demo(spark, base_opts)
    read_write_demo(spark, base_opts)


if __name__ == "__main__":
    main()
