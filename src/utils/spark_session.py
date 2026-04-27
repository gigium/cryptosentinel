from pyspark.sql import SparkSession


def get_spark(app_name: str = "cryptosentinel") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()
