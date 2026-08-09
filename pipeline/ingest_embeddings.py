# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "sentence-transformers",
# ]
# ///
# MAGIC %md
# MAGIC # Trip Planner — Destination Embeddings (Phase 3)
# MAGIC
# MAGIC Run this as an actual Databricks **notebook**, not the file-editor "Run"
# MAGIC button.
# MAGIC
# MAGIC **Before running**, add `sentence-transformers` to this notebook's
# MAGIC **Environment** side panel (the icon in the notebook toolbar → Libraries
# MAGIC → add `sentence-transformers`) and apply it — do **not** `%pip install`
# MAGIC it in a cell below. Installing torch (a large native dependency pulled in
# MAGIC by sentence-transformers) mid-script has crashed the kernel in practice;
# MAGIC the Environment panel installs it before the notebook's Python process
# MAGIC even starts, which avoids that. See README "Known limitations".
# MAGIC
# MAGIC Prerequisites:
# MAGIC - Phase 2 (`pipeline/ingest_destinations.py`) has run and
# MAGIC   `trip_planner.gold.destination_profile` has rows with real `profile_text`.
# MAGIC - The `lakebase-url` secret exists (from `db/secret.py`), or a
# MAGIC   `LAKEBASE_URL` env var is set for local/dev runs.
# MAGIC - `sentence-transformers` is installed via the Environment panel (see above).
# MAGIC
# MAGIC What this does: reads `gold.destination_profile`, chunks each
# MAGIC destination's `profile_text` (800 chars, 100 overlap sliding window),
# MAGIC embeds every chunk with `all-MiniLM-L6-v2` (384-dim, matching the
# MAGIC `destination_embeddings` schema), and upserts into Lakebase — deleting
# MAGIC any existing embeddings for a destination before writing fresh ones, so
# MAGIC re-running after Phase 2 refreshes a description never leaves stale
# MAGIC chunks behind.

# COMMAND ----------

# MAGIC %pip install pg8000 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import ssl
from urllib.parse import urlparse

import pg8000.dbapi as pg8000

try:
    from sentence_transformers import SentenceTransformer
except ImportError as e:
    raise ImportError(
        "sentence-transformers isn't available. Don't %pip install it in a "
        "cell here — add it via this notebook's Environment side panel "
        "instead (see the note at the top of this notebook), then re-run."
    ) from e

CATALOG = "trip_planner"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100


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

# COMMAND ----------

# MAGIC %md ## Chunking

# COMMAND ----------

def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    text = (text or "").strip()
    if not text:
        return []
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = end - overlap
    return chunks

# COMMAND ----------

# MAGIC %md ## Load the embedding model
# MAGIC First load takes a little while (downloading model weights); subsequent
# MAGIC runs in the same session are fast.

# COMMAND ----------

model = SentenceTransformer(MODEL_NAME)
print(f"Loaded {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md ## Read profiles and chunk

# COMMAND ----------

profiles = (
    spark.table(f"{CATALOG}.gold.destination_profile")
    .select("destination_id", "name", "profile_text")
    .collect()
)
print(f"Loaded {len(profiles)} destination profiles")

chunk_rows = []  # destination_id, chunk_index, chunk_text — embedding filled in after batch encode
skipped = []

for p in profiles:
    dest_id = str(p["destination_id"])
    text = p["profile_text"]
    if not text or not text.strip():
        skipped.append(p["name"])
        continue
    for i, c in enumerate(chunk_text(text)):
        chunk_rows.append({"destination_id": dest_id, "chunk_index": i, "chunk_text": c})

n_destinations = len({r["destination_id"] for r in chunk_rows})
print(f"Chunked into {len(chunk_rows)} chunks across {n_destinations} destinations")
if skipped:
    print(f"Skipped (empty profile_text): {skipped}")

# COMMAND ----------

# MAGIC %md ## Embed and write to Lakebase

# COMMAND ----------

def vector_literal(values):
    """pg8000 has no built-in pgvector adapter — format as pgvector's own
    text representation and cast with ::vector in the SQL instead."""
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"


def write_embeddings(rows):
    if not rows:
        return
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        # Delete existing embeddings for exactly the destinations we're about
        # to (re)write, so a destination whose description changed doesn't
        # end up with stale chunks left behind from a previous run.
        for dest_id in {r["destination_id"] for r in rows}:
            cur.execute(
                "DELETE FROM destination_embeddings WHERE destination_id = %s AND model_name = %s",
                (dest_id, MODEL_NAME),
            )
        for r in rows:
            cur.execute(
                """
                INSERT INTO destination_embeddings
                    (destination_id, chunk_index, chunk_text, embedding, model_name)
                VALUES (%s, %s, %s, %s::vector, %s)
                """,
                (r["destination_id"], r["chunk_index"], r["chunk_text"],
                 vector_literal(r["embedding"]), MODEL_NAME),
            )
        cur.close()
        conn.commit()
    finally:
        conn.close()


if chunk_rows:
    texts = [r["chunk_text"] for r in chunk_rows]
    embeddings = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)
    for r, emb in zip(chunk_rows, embeddings):
        r["embedding"] = emb.tolist()

    write_embeddings(chunk_rows)
    print(f"Wrote {len(chunk_rows)} embedding rows to Lakebase destination_embeddings "
          f"(model={MODEL_NAME})")
else:
    print("Nothing to embed — no destinations with non-empty profile_text.")

# COMMAND ----------

# MAGIC %md ## Verify
# MAGIC Run these in the Lakebase SQL editor after this notebook finishes.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Row counts per destination
# MAGIC SELECT destination_id, count(*) AS chunks
# MAGIC FROM destination_embeddings
# MAGIC GROUP BY destination_id;
# MAGIC
# MAGIC -- Spot-check the actual text that got embedded
# MAGIC SELECT destination_id, chunk_index, LEFT(chunk_text, 80) AS preview
# MAGIC FROM destination_embeddings
# MAGIC ORDER BY destination_id, chunk_index;
# MAGIC
# MAGIC -- Semantic search sanity check: embed a query the same way this notebook
# MAGIC -- does, then find the closest chunks by cosine distance (<=>).
# MAGIC -- (Run from a notebook — this is Python, not SQL, since the query text
# MAGIC -- needs to go through the same embedding model.)
# MAGIC ```

# COMMAND ----------

# Example semantic search — run after the cells above, or any time in a new
# session (re-run the model-loading and connection cells first).
def search_destinations(query, top_k=5):
    query_vec = model.encode([query], normalize_embeddings=True)[0].tolist()
    conn = get_lakebase_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT destination_id, chunk_index, chunk_text,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM destination_embeddings
            WHERE model_name = %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (vector_literal(query_vec), MODEL_NAME, vector_literal(query_vec), top_k),
        )
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


for row in search_destinations("a relaxed coastal city with good hiking nearby"):
    print(row)