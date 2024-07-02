import pytest
from integration.test_basespark import arangodb_client, database_conn, spark, endpoints, single_endpoint, adb_hostname


def pytest_addoption(parser):
    parser.addoption("--adb-datasource-jar", action="store", dest="datasource_jar_loc", required=True)
    parser.addoption("--adb-hostname", action="store", dest="adb_hostname", default="172.28.0.1")
