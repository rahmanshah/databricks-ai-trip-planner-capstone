"""
Lakebase connection helper for the trip-planner-ui Databricks App.

Same pg8000 pattern as mcp_server/lakebase_broker.py — pure Python driver,
no compiled extensions, avoiding the psycopg2-binary crash class documented
in the README. Secret is injected as a plain LAKEBASE_URL env var via
app.yaml's resources/valueFrom block, same as the MCP server.
"""

import os
import ssl
from urllib.parse import urlparse

import pg8000.dbapi as pg8000


def get_connection():
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


def query(sql, params=()):
    """SELECT helper — returns a list of dicts using cursor.description for
    column names, since pg8000 rows are plain tuples by default."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def execute(sql, params=()):
    """INSERT/UPDATE/DELETE helper — commits and returns nothing. For
    inserts that need the new row's id back, use execute_returning_id
    instead."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cur.close()
        conn.commit()
    finally:
        conn.close()


def execute_returning_id(sql, params=()):
    """For INSERT ... RETURNING id statements — commits and returns the
    new row's id as a string."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        new_id = str(cur.fetchone()[0])
        cur.close()
        conn.commit()
        return new_id
    finally:
        conn.close()
