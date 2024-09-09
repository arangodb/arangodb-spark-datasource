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
        "ssl.verifyHost": "false"
    }


def main():
    parser = ArgumentParser()
    parser.add_argument("--import-path", default=None)
    parser.add_argument("--password", default="test")
    parser.add_argument("--endpoints", default="localhost:8529")
    parser.add_argument("--ssl-enabled", default="false")
    parser.add_argument("--ssl-cert-value", default="LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURlekNDQW1PZ0F3SUJBZ0lFZURDelh6QU5CZ2txaGtpRzl3MEJBUXNGQURCdU1SQXdEZ1lEVlFRR0V3ZFYKYm10dWIzZHVNUkF3RGdZRFZRUUlFd2RWYm10dWIzZHVNUkF3RGdZRFZRUUhFd2RWYm10dWIzZHVNUkF3RGdZRApWUVFLRXdkVmJtdHViM2R1TVJBd0RnWURWUVFMRXdkVmJtdHViM2R1TVJJd0VBWURWUVFERXdsc2IyTmhiR2h2CmMzUXdIaGNOTWpBeE1UQXhNVGcxTVRFNVdoY05NekF4TURNd01UZzFNVEU1V2pCdU1SQXdEZ1lEVlFRR0V3ZFYKYm10dWIzZHVNUkF3RGdZRFZRUUlFd2RWYm10dWIzZHVNUkF3RGdZRFZRUUhFd2RWYm10dWIzZHVNUkF3RGdZRApWUVFLRXdkVmJtdHViM2R1TVJBd0RnWURWUVFMRXdkVmJtdHViM2R1TVJJd0VBWURWUVFERXdsc2IyTmhiR2h2CmMzUXdnZ0VpTUEwR0NTcUdTSWIzRFFFQkFRVUFBNElCRHdBd2dnRUtBb0lCQVFDMVdpRG5kNCt1Q21NRzUzOVoKTlpCOE53STBSWkYzc1VTUUdQeDNsa3FhRlRaVkV6TVpMNzZIWXZkYzlRZzdkaWZ5S3lRMDlSTFNwTUFMWDlldQpTc2VEN2JaR25mUUg1MkJuS2NUMDllUTN3aDdhVlE1c04yb215Z2RITEM3WDl1c250eEFmdjdOem12ZG9nTlhvCkpReVkvaFNaZmY3UklxV0g4Tm5BVUtranFPZTZCZjVMRGJ4SEtFU21yRkJ4T0NPbmhjcHZaV2V0d3BpUmRKVlAKd1VuNVA4MkNBWnpmaUJmbUJabkI3RDBsKy82Q3Y0ak11SDI2dUFJY2l4blZla0JRemwxUmd3Y3p1aVpmMk1HTwo2NHZETU1KSldFOUNsWkYxdVF1UXJ3WEY2cXdodVAxSG5raWk2d05iVHRQV2xHU2txZXV0cjAwNCtIemJmOEtuClJZNFBBZ01CQUFHaklUQWZNQjBHQTFVZERnUVdCQlRCcnY5QXd5bnQzQzVJYmFDTnlPVzV2NEROa1RBTkJna3EKaGtpRzl3MEJBUXNGQUFPQ0FRRUFJbTlyUHZEa1lwbXpwU0loUjNWWEc5WTcxZ3hSRHJxa0VlTHNNb0V5cUdudwovengxYkRDTmVHZzJQbmNMbFc2elRJaXBFQm9vaXhJRTlVN0t4SGdaeEJ5MEV0NkVFV3ZJVW1ucjZGNEYrZGJUCkQwNTBHSGxjWjdlT2VxWVRQWWVRQzUwMkcxRm80dGROaTRsRFA5TDlYWnBmN1ExUWltUkgycWFMUzAzWkZaYTIKdFk3YWgvUlFxWkw4RGt4eDgvemMyNXNnVEhWcHhvSzg1M2dsQlZCcy9FTk1peUdKV21BWFFheWV3WTNFUHQvOQp3R3dWNEttVTNkUERsZVFlWFNVR1BVSVNlUXhGankrakN3MjFwWXZpV1ZKVE5CQTlsNW55M0doRW1jbk9UL2dRCkhDdlZSTHlHTE1iYU1aNEpyUHdiK2FBdEJncmdlaUs0eGVTTU12cmJodz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K")
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
