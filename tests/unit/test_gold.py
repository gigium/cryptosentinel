from datetime import datetime, timedelta, timezone

import pytest

from src.transforms.gold import (
    compute_mcap_dominance,
    compute_macro_delta_rate,
    compute_price_velocity,
    compute_rsi_14,
    compute_vol_spike_ratio,
)

_BASE = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


def ts(offset_hours: float = 0) -> str:
    """Return an ISO 8601 string as stored by the ingestion layer."""
    return (_BASE + timedelta(hours=offset_hours)).isoformat()


class TestVolSpikeRatio:
    def test_ratio_equals_current_over_rolling_avg(self, spark):
        data = [
            (ts(0), "bitcoin", 1000.0),
            (ts(1), "bitcoin", 2000.0),
            (ts(2), "bitcoin", 3000.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "total_volume"])
        result = compute_vol_spike_ratio(df).orderBy("ingested_at").collect()
        # last row: avg(1000, 2000, 3000) = 2000; ratio = 3000/2000 = 1.5
        assert abs(result[-1]["vol_spike_ratio"] - 1.5) < 1e-9

    def test_single_row_ratio_is_one(self, spark):
        data = [(ts(0), "bitcoin", 5000.0)]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "total_volume"])
        result = compute_vol_spike_ratio(df).collect()
        assert abs(result[0]["vol_spike_ratio"] - 1.0) < 1e-9

    def test_partitioned_by_coin(self, spark):
        data = [
            (ts(0), "bitcoin", 1000.0),
            (ts(0), "ethereum", 500.0),
            (ts(1), "bitcoin", 3000.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "total_volume"])
        result = {r["coin_id"]: r for r in compute_vol_spike_ratio(df).orderBy("coin_id", "ingested_at").collect()}
        # bitcoin second row: avg(1000, 3000) = 2000; ratio = 3000/2000 = 1.5
        btc_rows = [r for r in compute_vol_spike_ratio(df).collect() if r["coin_id"] == "bitcoin"]
        btc_rows.sort(key=lambda r: r["ingested_at"])
        assert abs(btc_rows[-1]["vol_spike_ratio"] - 1.5) < 1e-9


def ts_min(offset_minutes: float = 0) -> str:
    """Return an ISO string offset by minutes (for sub-hour precision tests)."""
    return (_BASE + timedelta(minutes=offset_minutes)).isoformat()


class TestPriceVelocity:
    def test_velocity_computed_correctly(self, spark):
        # 15-minute interval, price goes from 100 to 110 (+10%)
        # velocity = 10% / 15min = 0.6667 %/min
        data = [
            (ts_min(0), "bitcoin", 100.0),
            (ts_min(15), "bitcoin", 110.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "current_price"])
        result = compute_price_velocity(df).orderBy("ingested_at").collect()
        assert result[0]["price_velocity"] is None
        expected = (110.0 - 100.0) / 100.0 * 100.0 / 15.0
        assert abs(result[1]["price_velocity"] - expected) < 1e-6

    def test_first_row_is_null(self, spark):
        data = [(ts(0), "bitcoin", 50000.0)]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "current_price"])
        result = compute_price_velocity(df).collect()
        assert result[0]["price_velocity"] is None

    def test_negative_velocity_on_price_drop(self, spark):
        data = [
            (ts_min(0), "bitcoin", 100.0),
            (ts_min(15), "bitcoin", 90.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "current_price"])
        result = compute_price_velocity(df).orderBy("ingested_at").collect()
        assert result[1]["price_velocity"] < 0


class TestRsi14:
    def _make_price_series(self, spark, prices: list[float], coin: str = "bitcoin"):
        data = [(ts(i), coin, float(p)) for i, p in enumerate(prices)]
        return spark.createDataFrame(data, ["ingested_at", "coin_id", "current_price"])

    def test_all_up_gives_rsi_100(self, spark):
        prices = [100 + i * 10 for i in range(15)]
        df = self._make_price_series(spark, prices)
        result = compute_rsi_14(df).orderBy("ingested_at").collect()
        assert abs(result[-1]["rsi_14"] - 100.0) < 1e-6

    def test_all_down_gives_rsi_zero(self, spark):
        prices = [200 - i * 10 for i in range(15)]
        df = self._make_price_series(spark, prices)
        result = compute_rsi_14(df).orderBy("ingested_at").collect()
        assert abs(result[-1]["rsi_14"] - 0.0) < 1e-6

    def test_first_row_rsi_is_100_or_null(self, spark):
        # First row: no delta, gain=0, loss=0, avg_loss=0 → RSI=100
        prices = [100.0, 110.0]
        df = self._make_price_series(spark, prices)
        result = compute_rsi_14(df).orderBy("ingested_at").collect()
        assert result[0]["rsi_14"] == 100.0

    def test_rsi_within_bounds(self, spark):
        import random
        random.seed(42)
        prices = [100.0 + random.uniform(-5, 5) * i for i in range(20)]
        df = self._make_price_series(spark, prices)
        result = compute_rsi_14(df).collect()
        for row in result:
            assert 0.0 <= row["rsi_14"] <= 100.0


class TestMacroDeltaRate:
    def test_delta_between_current_and_7d_ago(self, spark):
        # rate was 5.0, changed to 5.25 six days ago
        data = [
            (ts(-200), "bitcoin", 5.00),
            (ts(-144), "bitcoin", 5.25),  # 6 days ago
            (ts(0), "bitcoin", 5.25),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "fed_funds_rate"])
        result = compute_macro_delta_rate(df).orderBy("ingested_at").collect()
        last = result[-1]
        # 7d window prior to ts(0) includes ts(-144) (6d ago) and ts(-200) (>7d ago is excluded)
        # last non-null in that window = 5.25 → delta = 5.25 - 5.25 = 0.0
        assert abs(last["macro_delta_rate"] - 0.0) < 1e-9

    def test_null_when_no_prior_7d_data(self, spark):
        data = [(ts(0), "bitcoin", 5.0)]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "fed_funds_rate"])
        result = compute_macro_delta_rate(df).collect()
        assert result[0]["macro_delta_rate"] is None

    def test_detects_rate_change(self, spark):
        # rate was 5.0 eight days ago, is now 5.5
        data = [
            (ts(-192), "bitcoin", 5.0),   # 8 days ago — outside 7d window
            (ts(-168), "bitcoin", 5.0),   # exactly 7 days ago
            (ts(0), "bitcoin", 5.5),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "fed_funds_rate"])
        result = compute_macro_delta_rate(df).orderBy("ingested_at").collect()
        # rangeBetween(-7*86400, -1): window includes ts(-168) (exactly 7d * 3600 = 604800s before)
        last = result[-1]
        assert last["macro_delta_rate"] is not None
        assert abs(last["macro_delta_rate"] - 0.5) < 1e-6


class TestMcapDominance:
    def test_percentages_sum_to_100(self, spark):
        data = [
            (ts(0), "bitcoin", 1000.0),
            (ts(0), "ethereum", 500.0),
            (ts(0), "solana", 500.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "market_cap"])
        result = compute_mcap_dominance(df).collect()
        total = sum(r["mcap_dominance_pct"] for r in result)
        assert abs(total - 100.0) < 1e-6

    def test_dominant_coin_has_highest_pct(self, spark):
        data = [
            (ts(0), "bitcoin", 900.0),
            (ts(0), "ethereum", 100.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "market_cap"])
        result = {r["coin_id"]: r["mcap_dominance_pct"] for r in compute_mcap_dominance(df).collect()}
        assert result["bitcoin"] == 90.0
        assert result["ethereum"] == 10.0

    def test_partitioned_per_timestamp(self, spark):
        data = [
            (ts(0), "bitcoin", 800.0),
            (ts(0), "ethereum", 200.0),
            (ts(1), "bitcoin", 600.0),
            (ts(1), "ethereum", 400.0),
        ]
        df = spark.createDataFrame(data, ["ingested_at", "coin_id", "market_cap"])
        result = compute_mcap_dominance(df).collect()
        for row in result:
            assert row["mcap_dominance_pct"] is not None
        # All pcts should be between 0 and 100
        for row in result:
            assert 0 < row["mcap_dominance_pct"] < 100
