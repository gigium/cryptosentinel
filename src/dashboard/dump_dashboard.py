"""
Read the CryptoSentinel dashboard back from the Databricks API and pretty-print
the serialized spec. Run this to inspect what Databricks actually stored so we
can align the deploy script with the canonical format.

Usage:
    DATABRICKS_HOST=... DATABRICKS_TOKEN=... python src/dashboard/dump_dashboard.py
"""
import json
import os
import sys

import requests

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(f"{HOST}/api/2.0/lakeview/dashboards", headers=HEADERS, timeout=30)
resp.raise_for_status()

dashboard_id = None
for d in resp.json().get("dashboards", []):
    if d.get("display_name") == "CryptoSentinel" and d.get("lifecycle_state") != "TRASHED":
        dashboard_id = d["dashboard_id"]
        break

if not dashboard_id:
    print("Dashboard 'CryptoSentinel' not found.", file=sys.stderr)
    sys.exit(1)

full = requests.get(f"{HOST}/api/2.0/lakeview/dashboards/{dashboard_id}", headers=HEADERS, timeout=30).json()
raw = full.get("serialized_dashboard", "{}")
spec = json.loads(raw)
print(json.dumps(spec, indent=2))
