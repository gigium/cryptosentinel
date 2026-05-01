import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    inspect.getframeinfo(inspect.currentframe()).filename
))))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from src.utils.spark_session import get_spark


def load_silver() -> DataFrame:
    spark = get_spark()
    return spark.table("silver.crypto_enriched")


def compute_vol_spike_ratio(df: DataFrame) -> DataFrame:
    w30d = (
        Window.partitionBy("coin_id")
        .orderBy(F.col("ingested_at").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )
    avg_vol = F.avg("total_volume").over(w30d)
    return df.withColumn(
        "vol_spike_ratio",
        F.when(avg_vol > 0, F.col("total_volume") / avg_vol).otherwise(None),
    )


def compute_price_velocity(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("coin_id").orderBy("ingested_at")
    prev_price = F.lag("current_price").over(w)
    prev_time = F.lag(F.col("ingested_at").cast("long")).over(w)
    elapsed_minutes = (F.col("ingested_at").cast("long") - prev_time) / 60.0
    return df.withColumn(
        "price_velocity",
        F.when(
            prev_price.isNotNull() & (prev_price > 0) & (elapsed_minutes > 0),
            (F.col("current_price") - prev_price) / prev_price * 100.0 / elapsed_minutes,
        ).otherwise(None),
    )


def compute_rsi_14(df: DataFrame) -> DataFrame:
    w_order = Window.partitionBy("coin_id").orderBy("ingested_at")
    w14 = w_order.rowsBetween(-13, 0)

    df = df.withColumn("_prev_price", F.lag("current_price").over(w_order))
    df = df.withColumn("_delta", F.col("current_price") - F.col("_prev_price"))
    df = df.withColumn("_gain", F.when(F.col("_delta") > 0, F.col("_delta")).otherwise(F.lit(0.0)))
    df = df.withColumn("_loss", F.when(F.col("_delta") < 0, -F.col("_delta")).otherwise(F.lit(0.0)))
    df = df.withColumn("_avg_gain", F.avg("_gain").over(w14))
    df = df.withColumn("_avg_loss", F.avg("_loss").over(w14))
    df = df.withColumn(
        "rsi_14",
        F.when(F.col("_avg_loss") == 0, F.lit(100.0))
        .otherwise(100.0 - (100.0 / (1.0 + F.col("_avg_gain") / F.col("_avg_loss")))),
    )
    return df.drop("_prev_price", "_delta", "_gain", "_loss", "_avg_gain", "_avg_loss")


def compute_macro_delta_rate(df: DataFrame) -> DataFrame:
    # Most recent fed_funds_rate within the prior 7 days (exclusive of current row)
    w7d = (
        Window.partitionBy("coin_id")
        .orderBy(F.col("ingested_at").cast("long"))
        .rangeBetween(-7 * 86400, -1)
    )
    rate_7d_ago = F.last("fed_funds_rate", ignorenulls=True).over(w7d)
    return df.withColumn(
        "macro_delta_rate",
        F.when(
            F.col("fed_funds_rate").isNotNull() & rate_7d_ago.isNotNull(),
            F.col("fed_funds_rate") - rate_7d_ago,
        ).otherwise(None),
    )


def compute_mcap_dominance(df: DataFrame) -> DataFrame:
    w_ts = Window.partitionBy("ingested_at")
    total_mcap = F.sum("market_cap").over(w_ts)
    return df.withColumn(
        "mcap_dominance_pct",
        F.when(total_mcap > 0, F.col("market_cap") / total_mcap * 100.0).otherwise(None),
    )


def write_to_gold(df: DataFrame) -> None:
    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
        "ingestion_date"
    ).saveAsTable("gold.crypto_features")


def main():
    df = load_silver()
    df = compute_vol_spike_ratio(df)
    df = compute_price_velocity(df)
    df = compute_rsi_14(df)
    df = compute_macro_delta_rate(df)
    df = compute_mcap_dominance(df)
    write_to_gold(df)


if __name__ == "__main__":
    main()
