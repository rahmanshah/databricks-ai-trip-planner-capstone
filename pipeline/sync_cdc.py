# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CDC Sync: Lakebase → Delta (Phase 6)
# MAGIC
# MAGIC **One-time prerequisite, done via the UI, not in this notebook:** register
# MAGIC the Lakebase database as a Unity Catalog catalog —
# MAGIC Apps (top right) → Lakebase Postgres → Provisioned → select your instance
# MAGIC → Catalogs page → **Add catalog** → give it a name (e.g.
# MAGIC `lakebase_trip_planner` — deliberately different from the `trip_planner`
# MAGIC catalog, which holds the Spark pipeline's bronze/silver/gold data, not a
# MAGIC mirror of Postgres itself).
# MAGIC
# MAGIC This creates a **read-only** UC catalog mirroring your Postgres tables —
# MAGIC no manual `CREATE CONNECTION`/credentials needed, unlike federating an
# MAGIC arbitrary external Postgres database. Querying it requires **Serverless
# MAGIC SQL Warehouse** compute (Pro/Classic warehouses return a permission
# MAGIC error) — Free Edition's default serverless compute should satisfy this.
# MAGIC
# MAGIC **Why this exists instead of native Lakebase CDF:** the native
# MAGIC Change Data Feed preview is unreliable on Free Edition (see README
# MAGIC "Known limitations"). This notebook is the fallback — a standard
# MAGIC incremental-ETL pattern: read rows changed since the last watermark from
# MAGIC the registered catalog, `MERGE` into Delta history tables, enable
# MAGIC *native Delta* CDF (stable, GA) on those, and advance the watermark.
# MAGIC The watermark itself lives in Lakebase's `cdc_watermarks` table
# MAGIC (seeded back in Phase 1) and gets written back via `pg8000` directly,
# MAGIC since the registered catalog is read-only from Unity Catalog's side.
# MAGIC
# MAGIC Run this as a notebook, on a schedule (Databricks Job) once verified.

# COMMAND ----------

# MAGIC %pip install pg8000 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import ssl
from urllib.parse import urlparse

import pg8000.dbapi as pg8000
from pyspark.sql import functions as F
from delta.tables import DeltaTable

LAKEBASE_CATALOG = "lakebase_trip_planner"  # must match whatever you named it during registration
TARGET_CATALOG = "trip_planner"
TARGET_SCHEMA = "gold"

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema}")


def get_lakebase_connection():
    conn_str = os.environ.get("LAKEBASE_URL") or dbutils.secrets.get(
        scope="trip-planner", key="lakebase-url"
    )
    p = urlparse(conn_str)
    return pg8000.connect(
        user=p.username, password=p.password, host=p.hostname,
        port=p.port or 5432, database=p.path.lstrip("/"),
        ssl_context=ssl.create_default_context(),
    )


def get_watermark(table_name):
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT last_synced_at FROM cdc_watermarks WHERE table_name = %s", (table_name,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    finally:
        conn.close()


def set_watermark(table_name, ts):
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE cdc_watermarks SET last_synced_at = %s WHERE table_name = %s", (ts, table_name))
        cur.close()
        conn.commit()
    finally:
        conn.close()

# COMMAND ----------

# MAGIC %md ## Target Delta tables
# MAGIC Explicit DDL rather than inferring from the DataFrame on first write —
# MAGIC same defensive pattern as pipeline/ingest_destinations.py, even though
# MAGIC the federation reader's own Postgres->Spark type mapping is lower-risk
# MAGIC than the raw uuid.UUID objects pg8000 returns in our own notebooks.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.trips_history (
    id STRING, owner_id STRING, title STRING, start_date DATE, end_date DATE,
    status STRING, updated_at TIMESTAMP, is_deleted BOOLEAN, synced_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.activities_history (
    id STRING, destination_id STRING, name STRING, category STRING, is_outdoor BOOLEAN,
    duration_minutes INT, updated_at TIMESTAMP, is_deleted BOOLEAN, synced_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.itinerary_items_history (
    id STRING, trip_id STRING, activity_id STRING, scheduled_date DATE, scheduled_time STRING,
    position INT, status STRING, reschedule_reason STRING,
    updated_at TIMESTAMP, is_deleted BOOLEAN, synced_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.packing_items_history (
    id STRING, trip_id STRING, item_name STRING, category STRING, is_packed BOOLEAN,
    added_by STRING, updated_at TIMESTAMP, is_deleted BOOLEAN, synced_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.agent_actions_history (
    id STRING, trip_id STRING, tool_name STRING, tool_input STRING, tool_output STRING,
    status STRING, created_at TIMESTAMP, synced_at TIMESTAMP
) USING DELTA
""")

for t in ("trips_history", "activities_history", "itinerary_items_history",
          "packing_items_history", "agent_actions_history"):
    spark.sql(f"ALTER TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.{t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md ## Sync — updated_at-tracked tables
# MAGIC `trips`, `activities`, `itinerary_items`, `packing_items` all got
# MAGIC `updated_at` triggers and `is_deleted` back in Phase 1 specifically for
# MAGIC this. `agent_actions` is append-only and handled separately below.

# COMMAND ----------

WATCHED_TABLES = {
    "trips": ["id", "owner_id", "title", "start_date", "end_date", "status", "updated_at", "is_deleted"],
    "activities": ["id", "destination_id", "name", "category", "is_outdoor", "duration_minutes", "updated_at", "is_deleted"],
    "itinerary_items": ["id", "trip_id", "activity_id", "scheduled_date", "scheduled_time", "position",
                         "status", "reschedule_reason", "updated_at", "is_deleted"],
    "packing_items": ["id", "trip_id", "item_name", "category", "is_packed", "added_by", "updated_at", "is_deleted"],
}
UUID_COLUMNS = {"id", "owner_id", "destination_id", "trip_id", "activity_id"}

sync_summary = []

for table_name, columns in WATCHED_TABLES.items():
    watermark = get_watermark(table_name)
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}_history"

    source_df = spark.table(f"{LAKEBASE_CATALOG}.public.{table_name}").select(*columns)
    if watermark is not None:
        source_df = source_df.filter(F.col("updated_at") > F.lit(watermark))

    change_count = source_df.count()
    sync_summary.append((table_name, change_count))
    if change_count == 0:
        continue

    change_df = source_df.withColumn("synced_at", F.current_timestamp())
    for col in UUID_COLUMNS:
        if col in change_df.columns:
            change_df = change_df.withColumn(col, F.col(col).cast("string"))
    if "scheduled_time" in change_df.columns:
        change_df = change_df.withColumn("scheduled_time", F.col("scheduled_time").cast("string"))

    (DeltaTable.forName(spark, target_table).alias("t")
        .merge(change_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    max_updated_at = source_df.agg(F.max("updated_at")).collect()[0][0]
    set_watermark(table_name, max_updated_at)

# COMMAND ----------

# MAGIC %md ## Sync — agent_actions (append-only, tracked by created_at)

# COMMAND ----------

watermark = get_watermark("agent_actions")
target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.agent_actions_history"

source_df = (
    spark.table(f"{LAKEBASE_CATALOG}.public.agent_actions")
    .select("id", "trip_id", "tool_name", "tool_input", "tool_output", "status", "created_at")
)
if watermark is not None:
    source_df = source_df.filter(F.col("created_at") > F.lit(watermark))

change_count = source_df.count()
sync_summary.append(("agent_actions", change_count))

if change_count > 0:
    change_df = (
        source_df
        .withColumn("id", F.col("id").cast("string"))
        .withColumn("trip_id", F.col("trip_id").cast("string"))
        .withColumn("tool_input", F.col("tool_input").cast("string"))
        .withColumn("tool_output", F.col("tool_output").cast("string"))
        .withColumn("synced_at", F.current_timestamp())
    )
    # Append-only source — whenNotMatchedInsertAll only, nothing to update.
    (DeltaTable.forName(spark, target_table).alias("t")
        .merge(change_df.alias("s"), "t.id = s.id")
        .whenNotMatchedInsertAll()
        .execute())

    max_created_at = source_df.agg(F.max("created_at")).collect()[0][0]
    set_watermark("agent_actions", max_created_at)

# COMMAND ----------

print("=== CDC sync summary ===")
for name, count in sync_summary:
    print(f"  {name}: {count} changed row(s) synced")

# COMMAND ----------

# MAGIC %md ## Analytics — a first look at the synced history
# MAGIC Two quick queries: what the agent's actually been doing, and how the
# MAGIC itinerary has evolved. Both read from Delta tables now, not Lakebase.

# COMMAND ----------

spark.sql(f"""
SELECT tool_name, status, count(*) AS calls
FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.agent_actions_history
GROUP BY tool_name, status
ORDER BY calls DESC
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT status, is_deleted, count(*) AS items
FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.itinerary_items_history
GROUP BY status, is_deleted
ORDER BY status, is_deleted
""").display()