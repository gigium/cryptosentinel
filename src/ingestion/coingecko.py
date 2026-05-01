import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    inspect.getframeinfo(inspect.currentframe()).filename
))))

from datetime import datetime, timezone

import pandas as pd
import requests

from src.utils.spark_session import get_spark

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


def fetch_markets() -> list[dict]:
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
    }
    response = requests.get(COINGECKO_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def add_ingestion_metadata(data: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    for row in data:
        row["ingested_at"] = now.isoformat()
        row["ingestion_date"] = now.date().isoformat()
    return data


def to_dataframe(data: list[dict]):
    spark = get_spark()
    pdf = pd.DataFrame(data)
    for col in pdf.select_dtypes(include=["number"]).columns:
        pdf[col] = pdf[col].astype(float)
    return spark.createDataFrame(pdf)


def write_to_bronze(df) -> None:
    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    df.write.format("delta").mode("append").partitionBy("ingestion_date").saveAsTable(
        "bronze.coingecko_raw"
    )


def main():
    data = fetch_markets()
    data = add_ingestion_metadata(data)
    df = to_dataframe(data)
    write_to_bronze(df)


if __name__ == "__main__":
    main()
