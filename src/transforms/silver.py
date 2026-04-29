import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    inspect.getframeinfo(inspect.currentframe()).filename
))))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.spark_session import get_spark

COIN_SYMBOL_MAP = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
}


def load_bronze() -> tuple[DataFrame, DataFrame, DataFrame]:
    spark = get_spark()
    coingecko = spark.table("bronze.coingecko_raw")
    binance = spark.table("bronze.binance_raw")
    fred = spark.table("bronze.fred_raw")
    return coingecko, binance, fred


def clean_coingecko(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["id", "ingested_at"])
        .filter(F.col("current_price").isNotNull())
        .select(
            F.col("id").alias("coin_id"),
            F.col("symbol"),
            F.col("current_price").cast("double"),
            F.col("market_cap").cast("double"),
            F.col("total_volume").cast("double"),
            F.col("price_change_percentage_24h").cast("double"),
            F.col("circulating_supply").cast("double"),
            F.col("ingested_at"),
            F.col("ingestion_date"),
        )
    )


def clean_binance(df: DataFrame) -> DataFrame:
    symbol_map = F.create_map(*[
        F.lit(k) for pair in COIN_SYMBOL_MAP.items() for k in pair
    ])
    return (
        df.filter(F.col("interval") == "1h")
        .dropDuplicates(["symbol", "open_time"])
        .filter(F.col("volume").isNotNull())
        .withColumn("coin_id", symbol_map[F.col("symbol")])
        .filter(F.col("coin_id").isNotNull())
        .select(
            F.col("coin_id"),
            F.col("symbol"),
            F.col("open_time"),
            F.col("open").cast("double"),
            F.col("high").cast("double"),
            F.col("low").cast("double"),
            F.col("close").cast("double"),
            F.col("volume").cast("double"),
            F.col("num_trades").cast("long"),
            F.col("ingestion_date"),
        )
    )


def clean_fred(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["series_id", "date"])
        .filter(F.col("value").isNotNull())
        .select(
            F.col("indicator"),
            F.col("date"),
            F.col("value").cast("double"),
        )
    )


def pivot_fred(df: DataFrame) -> DataFrame:
    return df.groupBy("date").pivot("indicator", ["cpi", "fed_funds_rate", "treasury_10y"]).agg(
        F.first("value")
    )


def join_to_silver(
    coingecko: DataFrame,
    binance: DataFrame,
    fred_pivoted: DataFrame,
) -> DataFrame:
    enriched = coingecko.join(
        binance.select("coin_id", "open_time", "open", "high", "low", "close", "volume", "num_trades", "ingestion_date"),
        on=["coin_id", "ingestion_date"],
        how="left",
    )
    enriched = enriched.withColumn(
        "fred_date", F.date_format(F.col("ingestion_date"), "yyyy-MM-dd")
    )
    enriched = enriched.join(fred_pivoted, enriched.fred_date == fred_pivoted.date, how="left")
    return enriched.drop("fred_date", "date")


def write_to_silver(df: DataFrame) -> None:
    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
        "ingestion_date"
    ).saveAsTable("silver.crypto_enriched")


def main():
    coingecko, binance, fred = load_bronze()
    coingecko = clean_coingecko(coingecko)
    binance = clean_binance(binance)
    fred = clean_fred(fred)
    fred_pivoted = pivot_fred(fred)
    silver = join_to_silver(coingecko, binance, fred_pivoted)
    write_to_silver(silver)


if __name__ == "__main__":
    main()
