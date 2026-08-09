# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Trip Planner — Destination Ingestion Pipeline (Phase 2)
# MAGIC
# MAGIC Run this as an actual Databricks **notebook** (not the file-editor "Run"
# MAGIC button) — the `%pip install` + `dbutils.secrets` calls below expect
# MAGIC notebook context.
# MAGIC
# MAGIC Prerequisites (see README "Setup"):
# MAGIC - `check_environment.py` has confirmed outbound access to `open-meteo.com`
# MAGIC   and `*.wikimedia.org`.
# MAGIC - A `LAKEBASE_URL` secret exists (scope `trip-planner`, key `lakebase-url`),
# MAGIC   or a `LAKEBASE_URL` env var is set for local/dev runs.
# MAGIC - The `trip_planner` catalog exists in Unity Catalog.
# MAGIC
# MAGIC What this does: reads destinations from Lakebase that still need
# MAGIC coordinates or a real description, geocodes/fetches weather + air quality
# MAGIC + Wikipedia summary + nearby attractions for each, lands the raw API
# MAGIC responses in a bronze Delta table, upserts parsed results into silver
# MAGIC Delta tables via Spark, builds a gold `destination_profile` text column
# MAGIC (Phase 3's embedding source), and writes the enriched fields back into
# MAGIC Lakebase's `destinations` and `weather_snapshots` tables.

# COMMAND ----------

# MAGIC %pip install requests pg8000 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import re
import time
from datetime import date, timedelta

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)
from delta.tables import DeltaTable

CATALOG = "trip_planner"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

OPEN_METEO_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WIKIPEDIA_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
WIKIPEDIA_ACTION_API_URL = "https://en.wikipedia.org/w/api.php"

MAX_FORECAST_DAYS = 15  # Open-Meteo's 16-day forecast counts today as day one,
                          # so the furthest valid end_date is today+15, not today+16

# Required by Wikimedia's API etiquette — replace with a real contact before
# running for real, or requests may be throttled without warning.
WIKIMEDIA_USER_AGENT = os.environ.get(
    "WIKIMEDIA_USER_AGENT",
    "trip-planner-capstone/0.1 (rahman.shah@protonmail.ch)",
)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md ## Lakebase connection

# COMMAND ----------

import ssl
from urllib.parse import urlparse

import pg8000.dbapi as pg8000


def get_lakebase_connection():
    conn_str = os.environ.get("LAKEBASE_URL") or dbutils.secrets.get(
        scope="trip-planner", key="lakebase-url"
    )
    p = urlparse(conn_str)
    return pg8000.connect(
        user=p.username,
        password=p.password,
        host=p.hostname,
        port=p.port or 5432,
        database=p.path.lstrip("/"),
        ssl_context=ssl.create_default_context(),
    )


def fetch_pending_destinations():
    """Destinations still missing coordinates or a real (non-placeholder) description."""
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, latitude, longitude, description, arrival_date, departure_date
            FROM destinations
            WHERE is_deleted = false
            """
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()

# COMMAND ----------

# MAGIC %md ## API helpers
# MAGIC Each does one HTTP call and returns `(parsed_value, raw_json)` so the
# MAGIC raw response can always be landed in bronze regardless of how parsing
# MAGIC goes.

# COMMAND ----------

def _raise_with_body(resp):
    """Open-Meteo/Wikimedia return a JSON {"error": ..., "reason": ...} body on
    4xx responses — raise_for_status() alone discards it, which is exactly the
    detail needed to debug a bad parameter."""
    if not resp.ok:
        raise requests.HTTPError(f"{resp.status_code} for {resp.url}: {resp.text[:500]}")


def geocode_destination(name):
    resp = requests.get(
        OPEN_METEO_GEOCODE_URL,
        params={"name": name, "count": 1, "language": "en", "format": "json"},
        timeout=10,
    )
    _raise_with_body(resp)
    data = resp.json()
    results = data.get("results") or []
    if not results:
        return None, data
    top = results[0]
    return {"latitude": top["latitude"], "longitude": top["longitude"]}, data


def fetch_forecast(lat, lon, start_date, end_date):
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": "auto",
    }
    resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=10)
    _raise_with_body(resp)
    return resp.json()


def fetch_air_quality(lat, lon, start_date, end_date):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "us_aqi",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": "auto",
    }
    resp = requests.get(OPEN_METEO_AIR_QUALITY_URL, params=params, timeout=10)
    _raise_with_body(resp)
    return resp.json()


def _parse_allowed_end_date(error_text):
    """Extract the upper bound from Open-Meteo's own
    "... out of allowed range from X to Y" message, if present."""
    m = re.search(r"out of allowed range from \S+ to (\d{4}-\d{2}-\d{2})", error_text)
    return date.fromisoformat(m.group(1)) if m else None


def fetch_air_quality_safe(lat, lon, start, end):
    """Air quality's forecast horizon is shorter than the regular weather
    API's, and — as seen already — hardcoding a guessed day count risks
    drifting off by a day again. Self-corrects against whatever Open-Meteo
    actually reports as allowed instead. Returns None (not an exception) if
    AQI genuinely can't be fetched for this window, so callers can fall back
    to weather-without-AQI rather than losing the whole day's data."""
    try:
        return fetch_air_quality(lat, lon, start, end)
    except requests.HTTPError as e:
        allowed_end = _parse_allowed_end_date(str(e))
        if not allowed_end or allowed_end < start:
            return None
        try:
            return fetch_air_quality(lat, lon, start, min(end, allowed_end))
        except requests.HTTPError:
            return None


def fetch_wikipedia_summary(name):
    url = WIKIPEDIA_SUMMARY_URL.format(title=requests.utils.quote(name))
    resp = requests.get(url, headers={"User-Agent": WIKIMEDIA_USER_AGENT}, timeout=10)
    if resp.status_code == 404:
        return None, {"status": 404}
    _raise_with_body(resp)
    data = resp.json()
    return data.get("extract"), data


def fetch_nearby_attractions(lat, lon, limit=5, radius_m=8000):
    params = {
        "action": "query",
        "list": "geosearch",
        "gscoord": f"{lat}|{lon}",
        "gsradius": radius_m,
        "gslimit": limit,
        "format": "json",
    }
    resp = requests.get(
        WIKIPEDIA_ACTION_API_URL,
        params=params,
        headers={"User-Agent": WIKIMEDIA_USER_AGENT},
        timeout=10,
    )
    _raise_with_body(resp)
    data = resp.json()
    hits = data.get("query", {}).get("geosearch", [])
    return [
        {"title": h["title"], "distance_m": float(h["dist"]) if h.get("dist") is not None else None}
        for h in hits
    ], data

# COMMAND ----------

# MAGIC %md ## Fetch loop
# MAGIC Sequential and defensive on purpose — a handful of destinations at a
# MAGIC time, and one bad API call shouldn't kill the whole run.

# COMMAND ----------

destinations = fetch_pending_destinations()
print(f"Processing {len(destinations)} destinations")

today = date.today()
max_forecast_date = today + timedelta(days=MAX_FORECAST_DAYS)

bronze_records = []       # -> bronze.api_responses
weather_rows = []         # -> silver.weather_daily, then Lakebase weather_snapshots
attraction_rows = []      # -> silver.nearby_attractions
destination_updates = []  # -> silver.destinations_enriched, then Lakebase destinations
failures = []

for dest in destinations:
    dest_id, name = str(dest["id"]), dest["name"]
    lat, lon = dest["latitude"], dest["longitude"]

    if lat is None or lon is None:
        try:
            coords, raw = geocode_destination(name)
            bronze_records.append({"destination_id": dest_id, "source": "geocoding", "payload": json.dumps(raw)})
            if not coords:
                failures.append((dest_id, name, "geocoding returned no results"))
                continue
            lat, lon = float(coords["latitude"]), float(coords["longitude"])
            destination_updates.append({"id": dest_id, "latitude": lat, "longitude": lon, "description": None})
        except Exception as e:
            failures.append((dest_id, name, f"geocoding failed: {e}"))
            continue
        time.sleep(0.2)

    if not dest["description"] or dest["description"].startswith("Placeholder"):
        try:
            extract, raw = fetch_wikipedia_summary(name)
            bronze_records.append({"destination_id": dest_id, "source": "wikipedia_summary", "payload": json.dumps(raw)})
            if extract:
                existing = next((u for u in destination_updates if u["id"] == dest_id), None)
                if existing:
                    existing["description"] = extract
                else:
                    destination_updates.append({"id": dest_id, "latitude": lat, "longitude": lon, "description": extract})
        except Exception as e:
            failures.append((dest_id, name, f"wikipedia summary failed: {e}"))
        time.sleep(0.2)

    try:
        attractions, raw = fetch_nearby_attractions(lat, lon)
        bronze_records.append({"destination_id": dest_id, "source": "wikipedia_geosearch", "payload": json.dumps(raw)})
        for a in attractions:
            attraction_rows.append({"destination_id": dest_id, "attraction_name": a["title"], "distance_m": a["distance_m"]})
    except Exception as e:
        failures.append((dest_id, name, f"nearby attractions failed: {e}"))
    time.sleep(0.2)

    start = dest["arrival_date"] or (today + timedelta(days=7))
    end = dest["departure_date"] or (start + timedelta(days=3))
    start, end = max(start, today), min(end, max_forecast_date)

    if start > max_forecast_date:
        failures.append((dest_id, name, f"trip window starts beyond Open-Meteo's {MAX_FORECAST_DAYS}-day horizon, skipped weather"))
    else:
        try:
            forecast = fetch_forecast(lat, lon, start, end)
            bronze_records.append({"destination_id": dest_id, "source": "forecast", "payload": json.dumps(forecast)})
        except Exception as e:
            failures.append((dest_id, name, f"forecast failed: {e}"))
            forecast = None

        if forecast is not None:
            aqi_by_date = {}
            aq = fetch_air_quality_safe(lat, lon, start, end)
            if aq is not None:
                bronze_records.append({"destination_id": dest_id, "source": "air_quality", "payload": json.dumps(aq)})
                hourly = aq.get("hourly", {})
                for t, v in zip(hourly.get("time", []), hourly.get("us_aqi", [])):
                    if v is not None:
                        aqi_by_date.setdefault(t[:10], []).append(v)
            else:
                failures.append((dest_id, name, "air quality unavailable for this window — weather saved without AQI"))

            daily = forecast.get("daily", {})
            for i, d in enumerate(daily.get("time", [])):
                aqi_values = aqi_by_date.get(d, [])
                weather_rows.append({
                    "destination_id": dest_id,
                    "forecast_date": d,
                    "temperature_high_c": float(daily["temperature_2m_max"][i]) if daily["temperature_2m_max"][i] is not None else None,
                    "temperature_low_c": float(daily["temperature_2m_min"][i]) if daily["temperature_2m_min"][i] is not None else None,
                    "precipitation_probability_pct": int(daily["precipitation_probability_max"][i]) if daily["precipitation_probability_max"][i] is not None else None,
                    "weather_code": int(daily["weathercode"][i]) if daily["weathercode"][i] is not None else None,
                    "air_quality_index": int(round(sum(aqi_values) / len(aqi_values))) if aqi_values else None,
                })
        time.sleep(0.2)

print(f"Collected {len(bronze_records)} raw responses, {len(weather_rows)} weather rows, "
      f"{len(attraction_rows)} attraction rows, {len(destination_updates)} destination updates, "
      f"{len(failures)} failures")
for f in failures:
    print("  FAILED:", f)

# COMMAND ----------

# MAGIC %md ## Bronze — land every raw response as-is

# COMMAND ----------

if bronze_records:
    bronze_df = spark.createDataFrame(bronze_records).withColumn("fetched_at", F.current_timestamp())
    bronze_df.write.format("delta").mode("append").saveAsTable(f"{BRONZE}.api_responses")
    print(f"Wrote {bronze_df.count()} rows to {BRONZE}.api_responses")

# COMMAND ----------

# MAGIC %md ## Silver — parsed, upserted via Delta MERGE

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.weather_daily (
    destination_id STRING,
    forecast_date DATE,
    temperature_high_c DOUBLE,
    temperature_low_c DOUBLE,
    precipitation_probability_pct INT,
    weather_code INT,
    air_quality_index INT,
    updated_at TIMESTAMP
) USING DELTA
""")

WEATHER_ROW_SCHEMA = StructType([
    StructField("destination_id", StringType(), False),
    StructField("forecast_date", StringType(), False),
    StructField("temperature_high_c", DoubleType(), True),
    StructField("temperature_low_c", DoubleType(), True),
    StructField("precipitation_probability_pct", IntegerType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("air_quality_index", IntegerType(), True),
])

if weather_rows:
    weather_df = (
        spark.createDataFrame(weather_rows, schema=WEATHER_ROW_SCHEMA)
        .withColumn("forecast_date", F.to_date("forecast_date"))
        .withColumn("updated_at", F.current_timestamp())
    )
    (DeltaTable.forName(spark, f"{SILVER}.weather_daily").alias("t")
        .merge(weather_df.alias("s"), "t.destination_id = s.destination_id AND t.forecast_date = s.forecast_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged {weather_df.count()} rows into {SILVER}.weather_daily")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.destinations_enriched (
    destination_id STRING,
    name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    wikipedia_extract STRING,
    updated_at TIMESTAMP
) USING DELTA
""")

ENRICHED_ROW_SCHEMA = StructType([
    StructField("destination_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("wikipedia_extract", StringType(), True),
])

if destination_updates:
    name_by_id = {str(d["id"]): d["name"] for d in destinations}
    enriched_records = [
        {
            "destination_id": u["id"],
            "name": name_by_id.get(u["id"]),
            "latitude": u["latitude"],
            "longitude": u["longitude"],
            "wikipedia_extract": u["description"],
        }
        for u in destination_updates
    ]
    enriched_df = spark.createDataFrame(enriched_records, schema=ENRICHED_ROW_SCHEMA).withColumn("updated_at", F.current_timestamp())
    (DeltaTable.forName(spark, f"{SILVER}.destinations_enriched").alias("t")
        .merge(enriched_df.alias("s"), "t.destination_id = s.destination_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged {enriched_df.count()} rows into {SILVER}.destinations_enriched")

# COMMAND ----------

ATTRACTION_ROW_SCHEMA = StructType([
    StructField("destination_id", StringType(), False),
    StructField("attraction_name", StringType(), True),
    StructField("distance_m", DoubleType(), True),
])

if attraction_rows:
    attractions_df = spark.createDataFrame(attraction_rows, schema=ATTRACTION_ROW_SCHEMA).withColumn("updated_at", F.current_timestamp())
    # Overwrite each run — simplest correct behavior since attraction lists rarely change.
    attractions_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{SILVER}.nearby_attractions")
    print(f"Wrote {attractions_df.count()} rows to {SILVER}.nearby_attractions")

# COMMAND ----------

# MAGIC %md ## Gold — destination_profile text (Phase 3's embedding source)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.destination_profile (
    destination_id STRING,
    name STRING,
    profile_text STRING,
    updated_at TIMESTAMP
) USING DELTA
""")

dest_df = spark.table(f"{SILVER}.destinations_enriched")

if spark.catalog.tableExists(f"{SILVER}.nearby_attractions"):
    attr_agg = (
        spark.table(f"{SILVER}.nearby_attractions")
        .groupBy("destination_id")
        .agg(F.concat_ws(", ", F.collect_list("attraction_name")).alias("attraction_text"))
    )
    profile_df = dest_df.join(attr_agg, "destination_id", "left")
else:
    profile_df = dest_df.withColumn("attraction_text", F.lit(None).cast("string"))

profile_df = profile_df.select(
    "destination_id",
    "name",
    F.concat_ws(
        " ",
        F.coalesce(F.col("wikipedia_extract"), F.lit("")),
        F.when(F.col("attraction_text").isNotNull(), F.concat(F.lit("Nearby: "), F.col("attraction_text"))).otherwise(F.lit("")),
    ).alias("profile_text"),
).withColumn("updated_at", F.current_timestamp())

(DeltaTable.forName(spark, f"{GOLD}.destination_profile").alias("t")
    .merge(profile_df.alias("s"), "t.destination_id = s.destination_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
print(f"Merged {profile_df.count()} rows into {GOLD}.destination_profile")

# COMMAND ----------

# MAGIC %md ## Write enriched fields back to Lakebase
# MAGIC So the UI app and agent see fresh coordinates/descriptions/forecasts
# MAGIC without having to query Unity Catalog on every request.

# COMMAND ----------

def write_back_to_lakebase(destination_updates, weather_rows):
    if not destination_updates and not weather_rows:
        return
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        for u in destination_updates:
            cur.execute(
                """
                UPDATE destinations
                SET latitude = COALESCE(%s, latitude),
                    longitude = COALESCE(%s, longitude),
                    description = COALESCE(%s, description)
                WHERE id = %s
                """,
                (u["latitude"], u["longitude"], u["description"], u["id"]),
            )
        for w in weather_rows:
            cur.execute(
                """
                INSERT INTO weather_snapshots
                    (destination_id, forecast_date, temperature_high_c, temperature_low_c,
                     precipitation_probability_pct, air_quality_index, weather_code, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'open-meteo')
                ON CONFLICT (destination_id, forecast_date, source)
                DO UPDATE SET
                    temperature_high_c = EXCLUDED.temperature_high_c,
                    temperature_low_c = EXCLUDED.temperature_low_c,
                    precipitation_probability_pct = EXCLUDED.precipitation_probability_pct,
                    air_quality_index = EXCLUDED.air_quality_index,
                    weather_code = EXCLUDED.weather_code,
                    fetched_at = now()
                """,
                (
                    w["destination_id"], w["forecast_date"], w["temperature_high_c"], w["temperature_low_c"],
                    w["precipitation_probability_pct"], w["air_quality_index"], w["weather_code"],
                ),
            )
        cur.close()
        conn.commit()
    finally:
        conn.close()


write_back_to_lakebase(destination_updates, weather_rows)
print("Lakebase write-back complete.")