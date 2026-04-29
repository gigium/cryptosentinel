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

BINANCE_URL = "https://api.binance.us/api/v3/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVALS = ["1m", "1h"]


def fetch_klines(symbol: str, interval: str, limit: int = 15) -> list[dict]:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(BINANCE_URL, params=params, timeout=10)
    response.raise_for_status()
    return [_parse_kline(row, symbol, interval) for row in response.json()]


def _parse_kline(row: list, symbol: str, interval: str) -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "open_time": row[0],
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
        "close_time": row[6],
        "quote_asset_volume": float(row[7]),
        "num_trades": int(row[8]),
    }


def add_ingestion_metadata(data: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    for row in data:
        row["ingested_at"] = now.isoformat()
        row["ingestion_date"] = now.date().isoformat()
    return data


def fetch_all() -> list[dict]:
    rows = []
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            rows.extend(fetch_klines(symbol, interval))
    return rows


def write_to_bronze(data: list[dict]) -> None:
    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    df = spark.createDataFrame(pd.DataFrame(data))
    df.write.format("delta").mode("append").partitionBy("ingestion_date").saveAsTable(
        "bronze.binance_raw"
    )


def main():
    data = fetch_all()
    data = add_ingestion_metadata(data)
    write_to_bronze(data)


if __name__ == "__main__":
    main()
