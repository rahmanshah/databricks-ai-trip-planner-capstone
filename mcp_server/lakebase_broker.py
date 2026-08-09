"""
Lakebase data access for the MCP server's tools.

This module is deliberately "dumb" — reads and writes only, no scheduling
decisions (e.g. which day to reschedule an activity to). That logic belongs
in trip_mcp_server.py, which is the thin layer that actually decorates
functions with @mcp.tool and combines this module with weather_broker.py.

Connection note: unlike the notebook scripts (which read the Lakebase secret
via dbutils.secrets or WorkspaceClient), this runs inside a deployed
Databricks App, where the secret is injected as a plain LAKEBASE_URL
environment variable via app.yaml's resources/valueFrom block — no secrets
API call needed here.
"""

import json
import os
import ssl
import uuid as uuid_module
from urllib.parse import urlparse

import pg8000.dbapi as pg8000

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# pgvector's <=> always returns *something* as the top match regardless of
# actual relevance when the corpus is small — confirmed in Phase 3 testing,
# where a coastal-city query's top result was a Mount Rainier clouds/rain
# chunk at 21% similarity. Below this threshold, treat it as no match
# rather than confidently returning it. See README Known limitations.
SIMILARITY_THRESHOLD = 0.4

_model = None  # lazy singleton — loaded once per app instance, not per call


def get_lakebase_connection():
    conn_str = os.environ["LAKEBASE_URL"]
    p = urlparse(conn_str)
    return pg8000.connect(
        user=p.username,
        password=p.password,
        host=p.hostname,
        port=p.port or 5432,
        database=p.path.lstrip("/"),
        ssl_context=ssl.create_default_context(),
    )


def _in_clause(n):
    """WHERE x IN (%s,%s,...) placeholders — used instead of relying on
    ANY(%s) array-parameter binding, since that behaves differently across
    drivers and we've already been burned by driver-specific quirks once."""
    return "(" + ",".join(["%s"] * n) + ")"


def vector_literal(values):
    """pg8000 has no built-in pgvector adapter — format as pgvector's own
    text representation and cast with ::vector in the SQL instead."""
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"


def _is_valid_uuid(value):
    """Guards every id-lookup function below. Without this, a malformed or
    hallucinated id from the agent (seen in testing: the literal string
    "Seattle's destination ID" instead of a real uuid) hits Postgres
    directly and comes back as a raw 'invalid input syntax for type uuid'
    error instead of the clean not-found response callers expect."""
    try:
        uuid_module.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def get_embedding_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _model


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_destinations(query, top_k=5):
    """Embeds `query` with the same model Phase 3 used to build the index,
    runs a pgvector cosine search, and filters out anything below
    SIMILARITY_THRESHOLD rather than returning a confident-looking top-k
    regardless of whether any of it is actually relevant."""
    model = get_embedding_model()
    query_vec = model.encode([query], normalize_embeddings=True)[0].tolist()
    vec_literal = vector_literal(query_vec)

    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT e.destination_id, d.name, e.chunk_text,
                   1 - (e.embedding <=> %s::vector) AS similarity
            FROM destination_embeddings e
            JOIN destinations d ON d.id = e.destination_id
            WHERE d.is_deleted = false
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s
            """,
            (vec_literal, vec_literal, top_k),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    matches = [
        {"destination_id": str(r[0]), "name": r[1], "matched_text": r[2], "similarity": round(r[3], 4)}
        for r in rows
        if r[3] >= SIMILARITY_THRESHOLD
    ]
    if not matches:
        return {
            "matches": [],
            "message": (
                "No destination in this trip's index is a strong semantic match for "
                f"that query — the closest results were below the relevance threshold "
                f"({SIMILARITY_THRESHOLD:.0%})."
            ),
        }
    return {"matches": matches}


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_destination(destination_id):
    if not _is_valid_uuid(destination_id):
        return None
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, trip_id, name, latitude, longitude, arrival_date, departure_date
            FROM destinations WHERE id = %s AND is_deleted = false
            """,
            (destination_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "id": str(row[0]), "trip_id": str(row[1]), "name": row[2],
        "latitude": row[3], "longitude": row[4],
        "arrival_date": row[5], "departure_date": row[6],
    }


def get_itinerary_item(itinerary_item_id):
    if not _is_valid_uuid(itinerary_item_id):
        return None
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, trip_id, activity_id, scheduled_date, scheduled_time, position, status, reschedule_reason
            FROM itinerary_items WHERE id = %s AND is_deleted = false
            """,
            (itinerary_item_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "id": str(row[0]), "trip_id": str(row[1]), "activity_id": str(row[2]),
        "scheduled_date": row[3], "scheduled_time": str(row[4]) if row[4] else None,
        "position": row[5], "status": row[6], "reschedule_reason": row[7],
    }


def get_trip_overview(trip_id):
    """Trip + its destinations (each with nested activities and any cached
    weather_snapshots) + existing itinerary_items. The one read call
    generate_itinerary, build_packing_list, and reschedule_activity all
    build their logic on top of."""
    if not _is_valid_uuid(trip_id):
        return None
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT id, title, start_date, end_date, status FROM trips WHERE id = %s AND is_deleted = false",
            (trip_id,),
        )
        trip_row = cur.fetchone()
        if not trip_row:
            cur.close()
            return None
        trip = {
            "id": str(trip_row[0]), "title": trip_row[1],
            "start_date": trip_row[2], "end_date": trip_row[3], "status": trip_row[4],
        }

        cur.execute(
            """
            SELECT id, name, latitude, longitude, arrival_date, departure_date
            FROM destinations WHERE trip_id = %s AND is_deleted = false
            """,
            (trip_id,),
        )
        destinations = {
            str(r[0]): {
                "id": str(r[0]), "name": r[1], "latitude": r[2], "longitude": r[3],
                "arrival_date": r[4], "departure_date": r[5],
                "activities": [], "weather": [],
            }
            for r in cur.fetchall()
        }
        dest_ids = list(destinations.keys())

        if dest_ids:
            cur.execute(
                f"""
                SELECT id, destination_id, name, category, is_outdoor, duration_minutes
                FROM activities
                WHERE destination_id IN {_in_clause(len(dest_ids))} AND is_deleted = false
                """,
                tuple(dest_ids),
            )
            for r in cur.fetchall():
                dest_id = str(r[1])
                if dest_id in destinations:
                    destinations[dest_id]["activities"].append({
                        "id": str(r[0]), "name": r[2], "category": r[3],
                        "is_outdoor": r[4], "duration_minutes": r[5],
                    })

            cur.execute(
                f"""
                SELECT destination_id, forecast_date, temperature_high_c, temperature_low_c,
                       precipitation_probability_pct, air_quality_index
                FROM weather_snapshots
                WHERE destination_id IN {_in_clause(len(dest_ids))}
                """,
                tuple(dest_ids),
            )
            for r in cur.fetchall():
                dest_id = str(r[0])
                if dest_id in destinations:
                    destinations[dest_id]["weather"].append({
                        "forecast_date": r[1], "temperature_high_c": r[2],
                        "temperature_low_c": r[3], "precipitation_probability_pct": r[4],
                        "air_quality_index": r[5],
                    })

        cur.execute(
            """
            SELECT id, activity_id, scheduled_date, scheduled_time, position, status, reschedule_reason
            FROM itinerary_items WHERE trip_id = %s AND is_deleted = false
            ORDER BY scheduled_date, position
            """,
            (trip_id,),
        )
        itinerary_items = [
            {
                "id": str(r[0]), "activity_id": str(r[1]), "scheduled_date": r[2],
                "scheduled_time": str(r[3]) if r[3] else None, "position": r[4],
                "status": r[5], "reschedule_reason": r[6],
            }
            for r in cur.fetchall()
        ]
        cur.close()
    finally:
        conn.close()

    return {"trip": trip, "destinations": list(destinations.values()), "itinerary_items": itinerary_items}


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def log_agent_action(trip_id, tool_name, tool_input, tool_output, status="success"):
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO agent_actions (trip_id, tool_name, tool_input, tool_output, status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (trip_id, tool_name, json.dumps(tool_input), json.dumps(tool_output), status),
        )
        cur.close()
        conn.commit()
    finally:
        conn.close()


def insert_itinerary_items(rows):
    """rows: list of dicts with trip_id, activity_id, scheduled_date
    (datetime.date or 'YYYY-MM-DD'), and optionally scheduled_time,
    position, status. Returns the new ids."""
    if not rows:
        return []
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        new_ids = []
        for r in rows:
            cur.execute(
                """
                INSERT INTO itinerary_items
                    (trip_id, activity_id, scheduled_date, scheduled_time, position, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (r["trip_id"], r["activity_id"], r["scheduled_date"], r.get("scheduled_time"),
                 r.get("position", 1), r.get("status", "planned")),
            )
            new_ids.append(str(cur.fetchone()[0]))
        cur.close()
        conn.commit()
        return new_ids
    finally:
        conn.close()


_ITINERARY_UPDATABLE_FIELDS = {
    "scheduled_date", "scheduled_time", "position", "status", "reschedule_reason", "is_deleted",
}


def update_itinerary_item(itinerary_item_id, **fields):
    """Generic field updater backing reschedule_activity and
    move_or_remove_itinerary_item. Field names are checked against an
    allowlist before being interpolated into the SQL."""
    if not fields:
        return
    unknown = set(fields) - _ITINERARY_UPDATABLE_FIELDS
    if unknown:
        raise ValueError(f"Unknown itinerary_items field(s): {unknown}")

    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        cur.execute(
            f"UPDATE itinerary_items SET {set_clause} WHERE id = %s",
            tuple(fields.values()) + (itinerary_item_id,),
        )
        cur.close()
        conn.commit()
    finally:
        conn.close()


def soft_delete_itinerary_item(itinerary_item_id):
    update_itinerary_item(itinerary_item_id, is_deleted=True)


def insert_packing_items(trip_id, item_names, category="general", added_by="agent"):
    """Inserts items not already on the trip's list (case-insensitive dedup
    by item_name). Returns the names actually inserted."""
    if not item_names:
        return []
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT lower(item_name) FROM packing_items WHERE trip_id = %s AND is_deleted = false",
            (trip_id,),
        )
        existing = {r[0] for r in cur.fetchall()}
        inserted = []
        for name in item_names:
            if name.lower() in existing:
                continue
            cur.execute(
                "INSERT INTO packing_items (trip_id, item_name, category, added_by) VALUES (%s, %s, %s, %s)",
                (trip_id, name, category, added_by),
            )
            inserted.append(name)
            existing.add(name.lower())
        cur.close()
        conn.commit()
        return inserted
    finally:
        conn.close()