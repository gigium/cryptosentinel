"""
Idempotent Lakeview dashboard deployment.
Finds "CryptoSentinel" by name, updates it if it exists, creates it otherwise,
then publishes.

Usage:
    DATABRICKS_HOST=... DATABRICKS_TOKEN=... python src/dashboard/deploy_dashboard.py
"""
import json
import os
import sys

import requests

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
DASHBOARD_NAME = "CryptoSentinel"

# ─── Dashboard spec ────────────────────────────────────────────────────────────

def _fields(*names: str, disaggregated: bool = True) -> dict:
    """Build a query block with backtick-quoted field expressions."""
    return {
        "fields": [{"name": n, "expression": f"`{n}`"} for n in names],
        "disaggregated": disaggregated,
    }


DASHBOARD_SPEC = {
    "datasets": [
        {
            "name": "price_tracker",
            "displayName": "Price Tracker (24h)",
            "query": (
                "SELECT coin_id, symbol, ingested_at, current_price "
                "FROM gold.crypto_features "
                "WHERE ingested_at >= NOW() - INTERVAL 24 HOURS "
                "ORDER BY ingested_at"
            ),
        },
        {
            "name": "volume_spikes",
            "displayName": "Volume Spike Heatmap",
            "query": (
                "SELECT coin_id, symbol, "
                "MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 1 HOUR  THEN vol_spike_ratio END) AS spike_1h, "
                "MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 4 HOURS THEN vol_spike_ratio END) AS spike_4h, "
                "MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 24 HOURS THEN vol_spike_ratio END) AS spike_24h "
                "FROM gold.crypto_features "
                "WHERE ingested_at >= NOW() - INTERVAL 24 HOURS "
                "GROUP BY coin_id, symbol "
                "ORDER BY spike_1h DESC NULLS LAST"
            ),
        },
        {
            "name": "mcap_dominance",
            "displayName": "Market Cap Dominance (7d)",
            "query": (
                "SELECT coin_id, symbol, "
                "DATE_TRUNC('hour', ingested_at) AS hour, "
                "AVG(mcap_dominance_pct) AS avg_dominance_pct "
                "FROM gold.crypto_features "
                "WHERE coin_id IN ('bitcoin', 'ethereum') "
                "AND ingested_at >= NOW() - INTERVAL 7 DAYS "
                "GROUP BY coin_id, symbol, DATE_TRUNC('hour', ingested_at) "
                "ORDER BY hour"
            ),
        },
        {
            "name": "spike_alerts",
            "displayName": "Spike Alerts (>2.5x)",
            "query": (
                "SELECT coin_id, symbol, ingested_at, "
                "ROUND(vol_spike_ratio, 2) AS vol_spike_ratio, "
                "ROUND(price_change_percentage_24h, 2) AS price_delta_pct_24h, "
                "current_price, ROUND(rsi_14, 1) AS rsi_14 "
                "FROM gold.crypto_features "
                "WHERE vol_spike_ratio > 2.5 AND ingested_at >= NOW() - INTERVAL 24 HOURS "
                "ORDER BY ingested_at DESC, vol_spike_ratio DESC "
                "LIMIT 100"
            ),
        },
    ],
    "pages": [
        {
            "name": "overview",
            "displayName": "Overview",
            "layout": [
                # ── Panel 1: Price tracker (line chart) ────────────────────
                {
                    "widget": {
                        "name": "price_chart",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "price_tracker",
                                **_fields("ingested_at", "current_price", "symbol"),
                            },
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                # x must be temporal so Databricks renders a time axis
                                "x": {"fieldName": "ingested_at", "scale": {"type": "temporal"}},
                                # y is a single object for line charts (array only for bar)
                                "y": {"fieldName": "current_price", "scale": {"type": "quantitative"}},
                                "color": {"fieldName": "symbol", "scale": {"type": "categorical"}},
                            },
                            "frame": {"showTitle": True, "title": "Price Tracker (24h)"},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 6},
                },
                # ── Panel 2: Volume spike heatmap (table) ──────────────────
                {
                    "widget": {
                        "name": "spike_table",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "volume_spikes",
                                "disaggregated": True,
                            },
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "table",
                            "encodings": {},
                            "frame": {"showTitle": True, "title": "Volume Spike Heatmap (>2.5x amber, >5x red)"},
                        },
                    },
                    "position": {"x": 0, "y": 6, "width": 3, "height": 5},
                },
                # ── Panel 3: Market cap dominance (bar chart) ──────────────
                {
                    "widget": {
                        "name": "dominance_chart",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "mcap_dominance",
                                **_fields("hour", "avg_dominance_pct", "symbol"),
                            },
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "hour", "scale": {"type": "temporal"}},
                                # y is an array for bar charts
                                "y": [{"fieldName": "avg_dominance_pct", "scale": {"type": "quantitative"}}],
                                "color": {"fieldName": "symbol", "scale": {"type": "categorical"}},
                            },
                            "frame": {"showTitle": True, "title": "BTC / ETH Dominance (7d)"},
                        },
                    },
                    "position": {"x": 3, "y": 6, "width": 3, "height": 5},
                },
                # ── Panel 4: Spike alert table ──────────────────────────────
                {
                    "widget": {
                        "name": "alert_table",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "spike_alerts",
                                "disaggregated": True,
                            },
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "table",
                            "encodings": {},
                            "frame": {"showTitle": True, "title": "Spike Alert Feed (vol >2.5x)"},
                        },
                    },
                    "position": {"x": 0, "y": 11, "width": 6, "height": 5},
                },
            ],
        }
    ],
}

# ─── API helpers ───────────────────────────────────────────────────────────────

def _get(path: str, **kwargs) -> requests.Response:
    r = requests.get(f"{HOST}{path}", headers=HEADERS, timeout=30, **kwargs)
    r.raise_for_status()
    return r


def _post(path: str, body: dict) -> requests.Response:
    r = requests.post(f"{HOST}{path}", headers=HEADERS, json=body, timeout=30)
    r.raise_for_status()
    return r


def _patch(path: str, body: dict) -> requests.Response:
    r = requests.patch(f"{HOST}{path}", headers=HEADERS, json=body, timeout=30)
    r.raise_for_status()
    return r


def get_warehouse_id() -> str:
    warehouses = _get("/api/2.0/sql/warehouses").json().get("warehouses", [])
    if not warehouses:
        raise RuntimeError("No SQL warehouses found in workspace.")
    # Prefer running warehouses, fall back to the first one
    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    chosen = (running or warehouses)[0]
    print(f"Using warehouse: {chosen['name']} ({chosen['id']})")
    return chosen["id"]


def find_dashboard(name: str) -> str | None:
    data = _get("/api/2.0/lakeview/dashboards").json()
    for d in data.get("dashboards", []):
        if d.get("display_name") == name and d.get("lifecycle_state") != "TRASHED":
            return d["dashboard_id"]
    return None


def create_dashboard(name: str, spec: dict, warehouse_id: str) -> str:
    body = {
        "display_name": name,
        "serialized_dashboard": json.dumps(spec),
        "warehouse_id": warehouse_id,
    }
    return _post("/api/2.0/lakeview/dashboards", body).json()["dashboard_id"]


def update_dashboard(dashboard_id: str, name: str, spec: dict, warehouse_id: str) -> None:
    body = {
        "display_name": name,
        "serialized_dashboard": json.dumps(spec),
        "warehouse_id": warehouse_id,
    }
    _patch(f"/api/2.0/lakeview/dashboards/{dashboard_id}", body)


def publish_dashboard(dashboard_id: str, warehouse_id: str) -> None:
    _post(
        f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        {"embed_credentials": False, "warehouse_id": warehouse_id},
    )


# ─── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    warehouse_id = get_warehouse_id()
    dashboard_id = find_dashboard(DASHBOARD_NAME)

    if dashboard_id:
        print(f"Updating dashboard {dashboard_id} ...")
        update_dashboard(dashboard_id, DASHBOARD_NAME, DASHBOARD_SPEC, warehouse_id)
    else:
        print("Creating dashboard ...")
        dashboard_id = create_dashboard(DASHBOARD_NAME, DASHBOARD_SPEC, warehouse_id)

    try:
        publish_dashboard(dashboard_id, warehouse_id)
        print(f"Published: {HOST}/dashboards/{dashboard_id}")
    except requests.HTTPError as e:
        # Publishing is best-effort; Community Edition may not support it
        print(f"Warning: publish step failed ({e}). Dashboard saved as draft.", file=sys.stderr)
        print(f"Draft URL: {HOST}/dashboards/{dashboard_id}")


if __name__ == "__main__":
    main()
