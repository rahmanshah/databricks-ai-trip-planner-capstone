"""
Phase 0 sanity check — run this before pipeline/ingest_destinations.py.

Confirms:
  1. WIKIMEDIA_USER_AGENT has actually been edited away from the placeholder.
  2. Outbound internet reaches Open-Meteo (geocoding, forecast, air quality)
     and Wikimedia — Free Edition restricts outbound access to an allowlist
     until LinkedIn verification is done, so this is the one real unknown.
  3. The 'lakebase-url' secret (or a local LAKEBASE_URL env var) is set and
     the Lakebase Postgres connection + pgvector extension both work.

Run from a notebook cell in the Git folder:
    %sh python check_environment.py

Runs as a subprocess, not inside the notebook's own kernel — so it can't use
dbutils.secrets, and reads the secret via WorkspaceClient instead. Self-installs
its own dependencies, since each standalone file "Run" gets a fresh ephemeral
environment on Free Edition serverless compute.
"""

import base64
import os
import subprocess
import sys


def _ensure(package, import_name=None):
    try:
        __import__(import_name or package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", package])


_ensure("requests")
_ensure("psycopg2-binary", "psycopg2")
_ensure("databricks-sdk", "databricks.sdk")

import requests  # noqa: E402
import psycopg2  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

WIKIMEDIA_USER_AGENT = os.environ.get(
    "WIKIMEDIA_USER_AGENT",
    "trip-planner-capstone/0.1 (REPLACE_WITH_YOUR_CONTACT_EMAIL)",
)

SCOPE, KEY = "trip-planner", "lakebase-url"
results = []  # (name, ok, detail)


def check(name, fn):
    try:
        detail = fn()
        results.append((name, True, detail or "ok"))
    except Exception as e:
        results.append((name, False, str(e)))


def get_lakebase_url():
    url = os.environ.get("LAKEBASE_URL")
    if url:
        return url
    secret = WorkspaceClient().secrets.get_secret(scope=SCOPE, key=KEY)
    return base64.b64decode(secret.value).decode("utf-8")


def check_user_agent():
    if "REPLACE_WITH_YOUR_CONTACT_EMAIL" in WIKIMEDIA_USER_AGENT or "example.com" in WIKIMEDIA_USER_AGENT:
        raise RuntimeError(f"still a placeholder ({WIKIMEDIA_USER_AGENT!r}) — edit it in ingest_destinations.py")
    return WIKIMEDIA_USER_AGENT


def check_open_meteo_geocoding():
    r = requests.get("https://geocoding-api.open-meteo.com/v1/search",
                      params={"name": "Seattle", "count": 1}, timeout=10)
    r.raise_for_status()
    return f"HTTP {r.status_code}"


def check_open_meteo_forecast():
    r = requests.get("https://api.open-meteo.com/v1/forecast",
                      params={"latitude": 47.6, "longitude": -122.3, "daily": "weathercode",
                              "forecast_days": 1, "timezone": "auto"}, timeout=10)
    r.raise_for_status()
    return f"HTTP {r.status_code}"


def check_open_meteo_air_quality():
    r = requests.get("https://air-quality-api.open-meteo.com/v1/air-quality",
                      params={"latitude": 47.6, "longitude": -122.3, "hourly": "us_aqi",
                              "forecast_days": 1}, timeout=10)
    r.raise_for_status()
    return f"HTTP {r.status_code}"


def check_wikimedia():
    r = requests.get("https://en.wikipedia.org/api/rest_v1/page/summary/Seattle",
                      headers={"User-Agent": WIKIMEDIA_USER_AGENT}, timeout=10)
    r.raise_for_status()
    return f"HTTP {r.status_code}"


def check_lakebase_connection():
    conn = psycopg2.connect(get_lakebase_url())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()
    return "connected"


def check_pgvector():
    conn = psycopg2.connect(get_lakebase_url())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("vector extension not found — did schema.sql run?")
            return f"vector {row[0]}"
    finally:
        conn.close()


check("Wikimedia User-Agent configured", check_user_agent)
check("Open-Meteo geocoding reachable", check_open_meteo_geocoding)
check("Open-Meteo forecast reachable", check_open_meteo_forecast)
check("Open-Meteo air quality reachable", check_open_meteo_air_quality)
check("Wikimedia REST API reachable", check_wikimedia)
check("Lakebase connection", check_lakebase_connection)
check("pgvector extension", check_pgvector)

print("\n=== Phase 0 environment check ===")
all_ok = True
for name, ok, detail in results:
    status = "PASS" if ok else "FAIL"
    if not ok:
        all_ok = False
    print(f"[{status}] {name}: {detail}")

print(
    "\nAll checks passed — safe to run pipeline/ingest_destinations.py."
    if all_ok else
    "\nFix the FAILs above before running pipeline/ingest_destinations.py."
)
