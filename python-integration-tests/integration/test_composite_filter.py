import arango.database
import pyspark.sql
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

from integration import test_basespark

data = [
    {
        "integer": 1,
        "string": "one",
        "bool": True
    },
    {
        "integer": 2,
        "string": "two",
        "bool": True
    }
]

schema = StructType([
    StructField("integer", IntegerType(), nullable=False),
    StructField("string", StringType(), nullable=False),
    StructField("bool", BooleanType(), nullable=False),
])

table_name = "compositeFilter"


@pytest.fixture(scope="module")
def composite_df(database_conn: arango.database.StandardDatabase, spark: SparkSession) -> pyspark.sql.DataFrame:
    df = test_basespark.create_df(database_conn, spark, table_name, data, schema)
    yield df
    test_basespark.drop_table(database_conn, table_name)


def test_or_filter(spark: SparkSession, composite_df: pyspark.sql.DataFrame):
    field_name = "integer"
    value = data[0][field_name]
    res = composite_df.filter((col(field_name) == 0) | (col(field_name) == 1)).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
    SELECT * FROM {table_name}
    WHERE {field_name} = 0 OR {field_name} = 1
    """).collect()

    assert len(sql_res) == 1
    assert sql_res[0].asDict()[field_name] == value


def test_not_filter(spark: SparkSession, composite_df: pyspark.sql.DataFrame):
    field_name = "integer"
    value = data[0][field_name]
    res = composite_df.filter(~(col(field_name) == 2)).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name] == value

    sql_res = spark.sql(f"""
    SELECT * FROM {table_name}
    WHERE NOT {field_name} = 2
    """).collect()

    assert len(sql_res) == 1
    assert sql_res[0].asDict()[field_name] == value


def test_or_and_filter(spark: SparkSession, composite_df: pyspark.sql.DataFrame):
    field_name_1 = "integer"
    value_1 = data[0][field_name_1]

    field_name_2 = "string"
    value_2 = data[0][field_name_2]

    res = composite_df.filter((col("bool") == False) | ((col(field_name_1) == value_1) & (col(field_name_2) == value_2))).collect()

    assert len(res) == 1
    assert res[0].asDict()[field_name_1] == value_1

    sql_res = spark.sql(f"""
    SELECT * FROM {table_name}
    WHERE bool = false OR ({field_name_1} = {value_1} AND {field_name_2} = "{value_2}") 
    """).collect()

    assert len(sql_res) == 1
    assert sql_res[0].asDict()[field_name_1] == value_1
