import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local")
        .appName("cryptosentinel-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
