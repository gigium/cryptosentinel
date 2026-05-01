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

DASHBOARD_SPEC = {
    "datasets": [
        {
            "name": "price_tracker",
            "displayName": "Price Tracker (24h)",
            "query": """
                SELECT coin_id, symbol, ingested_at, current_price
                FROM gold.crypto_features
                WHERE ingested_at >= NOW() - INTERVAL 24 HOURS
                ORDER BY ingested_at
            """.strip(),
        },
        {
            "name": "volume_spikes",
            "displayName": "Volume Spike Heatmap",
            "query": """
                SELECT
                    coin_id, symbol,
                    MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 1 HOUR  THEN vol_spike_ratio END) AS spike_1h,
                    MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 4 HOURS THEN vol_spike_ratio END) AS spike_4h,
                    MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 24 HOURS THEN vol_spike_ratio END) AS spike_24h
                FROM gold.crypto_features
                WHERE ingested_at >= NOW() - INTERVAL 24 HOURS
                GROUP BY coin_id, symbol
                ORDER BY spike_1h DESC NULLS LAST
            """.strip(),
        },
        {
            "name": "mcap_dominance",
            "displayName": "Market Cap Dominance (7d)",
            "query": """
                SELECT
                    coin_id, symbol,
                    DATE_TRUNC('hour', ingested_at) AS hour,
                    AVG(mcap_dominance_pct) AS avg_dominance_pct
                FROM gold.crypto_features
                WHERE coin_id IN ('bitcoin', 'ethereum')
                  AND ingested_at >= NOW() - INTERVAL 7 DAYS
                GROUP BY coin_id, symbol, DATE_TRUNC('hour', ingested_at)
                ORDER BY hour
            """.strip(),
        },
        {
            "name": "spike_alerts",
            "displayName": "Spike Alerts (>2.5×)",
            "query": """
                SELECT
                    coin_id, symbol, ingested_at,
                    ROUND(vol_spike_ratio, 2)             AS vol_spike_ratio,
                    ROUND(price_change_percentage_24h, 2) AS price_delta_pct_24h,
                    current_price,
                    ROUND(rsi_14, 1)                      AS rsi_14
                FROM gold.crypto_features
                WHERE vol_spike_ratio > 2.5
                  AND ingested_at >= NOW() - INTERVAL 24 HOURS
                ORDER BY ingested_at DESC, vol_spike_ratio DESC
                LIMIT 100
            """.strip(),
        },
    ],
    "pages": [
        {
            "name": "overview",
            "displayName": "Overview",
            "layout": [
                # ── Panel 1: Price tracker ──────────────────────────────────
                {
                    "widget": {
                        "name": "price_chart",
                        "queries": [
                            {
                                "name": "q_price",
                                "query": {
                                    "datasetName": "price_tracker",
                                    "fields": [
                                        {"name": "ingested_at", "expression": "ingested_at"},
                                        {"name": "current_price", "expression": "current_price"},
                                        {"name": "symbol", "expression": "symbol"},
                                    ],
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {
                                    "fieldName": "ingested_at",
                                    "scale": {"type": "temporal"},
                                    "displayName": "Time",
                                },
                                "y": [
                                    {
                                        "fieldName": "current_price",
                                        "scale": {"type": "quantitative"},
                                        "displayName": "Price (USD)",
                                    }
                                ],
                                "color": {
                                    "fieldName": "symbol",
                                    "scale": {"type": "categorical"},
                                },
                            },
                            "frame": {"showTitle": True, "title": "Price Tracker (24h)"},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 6},
                },
                # ── Panel 2: Volume spike heatmap ───────────────────────────
                {
                    "widget": {
                        "name": "spike_table",
                        "queries": [
                            {
                                "name": "q_spikes",
                                "query": {
                                    "datasetName": "volume_spikes",
                                    "fields": [
                                        {"name": "coin_id", "expression": "coin_id"},
                                        {"name": "spike_1h", "expression": "spike_1h"},
                                        {"name": "spike_4h", "expression": "spike_4h"},
                                        {"name": "spike_24h", "expression": "spike_24h"},
                                    ],
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "coin_id", "displayName": "Coin"},
                                    {"fieldName": "spike_1h", "displayName": "1h Spike"},
                                    {"fieldName": "spike_4h", "displayName": "4h Spike"},
                                    {"fieldName": "spike_24h", "displayName": "24h Spike"},
                                ]
                            },
                            "frame": {
                                "showTitle": True,
                                "title": "Volume Spike Heatmap (amber >2.5×, red >5×)",
                            },
                        },
                    },
                    "position": {"x": 0, "y": 6, "width": 3, "height": 5},
                },
                # ── Panel 3: Market cap dominance ───────────────────────────
                {
                    "widget": {
                        "name": "dominance_chart",
                        "queries": [
                            {
                                "name": "q_dom",
                                "query": {
                                    "datasetName": "mcap_dominance",
                                    "fields": [
                                        {"name": "hour", "expression": "hour"},
                                        {"name": "avg_dominance_pct", "expression": "avg_dominance_pct"},
                                        {"name": "symbol", "expression": "symbol"},
                                    ],
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {
                                    "fieldName": "hour",
                                    "scale": {"type": "temporal"},
                                    "displayName": "Hour",
                                },
                                "y": [
                                    {
                                        "fieldName": "avg_dominance_pct",
                                        "scale": {"type": "quantitative"},
                                        "displayName": "Dominance %",
                                    }
                                ],
                                "color": {
                                    "fieldName": "symbol",
                                    "scale": {"type": "categorical"},
                                },
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
                        "queries": [
                            {
                                "name": "q_alerts",
                                "query": {
                                    "datasetName": "spike_alerts",
                                    "fields": [
                                        {"name": "coin_id", "expression": "coin_id"},
                                        {"name": "ingested_at", "expression": "ingested_at"},
                                        {"name": "vol_spike_ratio", "expression": "vol_spike_ratio"},
                                        {"name": "price_delta_pct_24h", "expression": "price_delta_pct_24h"},
                                        {"name": "current_price", "expression": "current_price"},
                                        {"name": "rsi_14", "expression": "rsi_14"},
                                    ],
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "coin_id", "displayName": "Coin"},
                                    {"fieldName": "ingested_at", "displayName": "Time"},
                                    {"fieldName": "vol_spike_ratio", "displayName": "Spike Ratio"},
                                    {"fieldName": "price_delta_pct_24h", "displayName": "Δ% 24h"},
                                    {"fieldName": "current_price", "displayName": "Price (USD)"},
                                    {"fieldName": "rsi_14", "displayName": "RSI"},
                                ]
                            },
                            "frame": {"showTitle": True, "title": "Spike Alert Feed (vol >2.5×)"},
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


def find_dashboard(name: str) -> str | None:
    data = _get("/api/2.0/lakeview/dashboards").json()
    for d in data.get("dashboards", []):
        if d.get("display_name") == name and d.get("lifecycle_state") != "TRASHED":
            return d["dashboard_id"]
    return None


def create_dashboard(name: str, spec: dict) -> str:
    body = {"display_name": name, "serialized_dashboard": json.dumps(spec)}
    return _post("/api/2.0/lakeview/dashboards", body).json()["dashboard_id"]


def update_dashboard(dashboard_id: str, name: str, spec: dict) -> None:
    body = {"display_name": name, "serialized_dashboard": json.dumps(spec)}
    _patch(f"/api/2.0/lakeview/dashboards/{dashboard_id}", body)


def publish_dashboard(dashboard_id: str) -> None:
    _post(f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", {"embed_credentials": False})


# ─── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    dashboard_id = find_dashboard(DASHBOARD_NAME)

    if dashboard_id:
        print(f"Updating dashboard {dashboard_id} ...")
        update_dashboard(dashboard_id, DASHBOARD_NAME, DASHBOARD_SPEC)
    else:
        print("Creating dashboard ...")
        dashboard_id = create_dashboard(DASHBOARD_NAME, DASHBOARD_SPEC)

    try:
        publish_dashboard(dashboard_id)
        print(f"Published: {HOST}/dashboards/{dashboard_id}")
    except requests.HTTPError as e:
        # Publishing is best-effort; Community Edition may not support it
        print(f"Warning: publish step failed ({e}). Dashboard saved as draft.", file=sys.stderr)
        print(f"Draft URL: {HOST}/dashboards/{dashboard_id}")


if __name__ == "__main__":
    main()
