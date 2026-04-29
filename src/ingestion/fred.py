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

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
SERIES = {
    "FEDFUNDS": "fed_funds_rate",
    "CPIAUCSL": "cpi",
    "DGS10": "treasury_10y",
}


def fetch_series(series_id: str, api_key: str, limit: int = 10) -> list[dict]:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "limit": limit,
        "sort_order": "desc",
    }
    response = requests.get(FRED_URL, params=params, timeout=10)
    response.raise_for_status()
    observations = response.json()["observations"]
    return [
        {
            "series_id": series_id,
            "indicator": SERIES[series_id],
            "date": obs["date"],
            "value": None if obs["value"] == "." else float(obs["value"]),
        }
        for obs in observations
    ]


def add_ingestion_metadata(data: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    for row in data:
        row["ingested_at"] = now.isoformat()
        row["ingestion_date"] = now.date().isoformat()
    return data


def fetch_all(api_key: str) -> list[dict]:
    rows = []
    for series_id in SERIES:
        rows.extend(fetch_series(series_id, api_key))
    return rows


def write_to_bronze(data: list[dict]) -> None:
    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    df = spark.createDataFrame(pd.DataFrame(data))
    df.write.format("delta").mode("append").partitionBy("ingestion_date").saveAsTable(
        "bronze.fred_raw"
    )


def main():
    api_key = sys.argv[1]
    data = fetch_all(api_key)
    data = add_ingestion_metadata(data)
    write_to_bronze(data)


if __name__ == "__main__":
    main()
