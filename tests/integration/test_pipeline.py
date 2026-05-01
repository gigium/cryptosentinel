"""
Integration tests — require DATABRICKS_HOST and DATABRICKS_TOKEN env vars.
The deploy workflow runs ingestion_job and transform_job before invoking these.
"""
import os
import time

import pytest
import requests

HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

_POLL_INTERVAL = 5
_POLL_TIMEOUT = 120


def _first_running_cluster() -> str:
    resp = requests.get(f"{HOST}/api/2.0/clusters/list", headers=HEADERS, timeout=30)
    resp.raise_for_status()
    clusters = resp.json().get("clusters", [])
    running = [c for c in clusters if c["state"] in ("RUNNING", "RESIZING")]
    if not running:
        raise RuntimeError("No running cluster found — has the pipeline job completed?")
    return running[0]["cluster_id"]


def _run_sql(cluster_id: str, ctx_id: str, query: str) -> list:
    cmd = requests.post(
        f"{HOST}/api/1.2/commands/execute",
        headers=HEADERS,
        json={"clusterId": cluster_id, "contextId": ctx_id, "language": "sql", "command": query},
        timeout=30,
    ).json()
    cmd_id = cmd["id"]

    deadline = time.time() + _POLL_TIMEOUT
    while time.time() < deadline:
        time.sleep(_POLL_INTERVAL)
        status = requests.get(
            f"{HOST}/api/1.2/commands/status",
            headers=HEADERS,
            params={"clusterId": cluster_id, "contextId": ctx_id, "commandId": cmd_id},
            timeout=30,
        ).json()
        if status["status"] == "Finished":
            if status["results"]["resultType"] == "error":
                raise RuntimeError(f"SQL error: {status['results']['summary']}\nQuery: {query}")
            return status["results"].get("data", [])
    raise TimeoutError(f"SQL timed out: {query}")


@pytest.fixture(scope="session")
def sql_ctx():
    cluster_id = _first_running_cluster()
    ctx = requests.post(
        f"{HOST}/api/1.2/contexts/create",
        headers=HEADERS,
        json={"clusterId": cluster_id, "language": "sql"},
        timeout=60,
    ).json()
    ctx_id = ctx["id"]
    yield cluster_id, ctx_id
    requests.post(
        f"{HOST}/api/1.2/contexts/destroy",
        headers=HEADERS,
        json={"clusterId": cluster_id, "contextId": ctx_id},
        timeout=30,
    )


@pytest.mark.integration
class TestBronzeTables:
    def test_coingecko_raw_has_rows(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(cluster_id, ctx_id, "SELECT COUNT(*) FROM bronze.coingecko_raw")
        assert int(rows[0][0]) > 0

    def test_binance_raw_has_rows(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(cluster_id, ctx_id, "SELECT COUNT(*) FROM bronze.binance_raw")
        assert int(rows[0][0]) > 0

    def test_fred_raw_has_rows(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(cluster_id, ctx_id, "SELECT COUNT(*) FROM bronze.fred_raw")
        assert int(rows[0][0]) > 0


@pytest.mark.integration
class TestSilverTable:
    def test_crypto_enriched_has_rows(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(cluster_id, ctx_id, "SELECT COUNT(*) FROM silver.crypto_enriched")
        assert int(rows[0][0]) > 0

    def test_no_null_prices(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(
            cluster_id, ctx_id,
            "SELECT COUNT(*) FROM silver.crypto_enriched WHERE current_price IS NULL",
        )
        assert int(rows[0][0]) == 0


@pytest.mark.integration
class TestGoldTable:
    def test_crypto_features_has_rows(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(cluster_id, ctx_id, "SELECT COUNT(*) FROM gold.crypto_features")
        assert int(rows[0][0]) > 0

    def test_vol_spike_ratio_is_non_null(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(
            cluster_id, ctx_id,
            "SELECT COUNT(*) FROM gold.crypto_features WHERE vol_spike_ratio IS NOT NULL",
        )
        assert int(rows[0][0]) > 0

    def test_rsi_within_bounds(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(
            cluster_id, ctx_id,
            "SELECT COUNT(*) FROM gold.crypto_features WHERE rsi_14 < 0 OR rsi_14 > 100",
        )
        assert int(rows[0][0]) == 0

    def test_mcap_dominance_sums_to_100(self, sql_ctx):
        cluster_id, ctx_id = sql_ctx
        rows = _run_sql(
            cluster_id, ctx_id,
            """
            SELECT MAX(ABS(total - 100)) AS max_deviation
            FROM (
                SELECT ingested_at, SUM(mcap_dominance_pct) AS total
                FROM gold.crypto_features
                WHERE mcap_dominance_pct IS NOT NULL
                GROUP BY ingested_at
            )
            """,
        )
        assert float(rows[0][0]) < 0.01
