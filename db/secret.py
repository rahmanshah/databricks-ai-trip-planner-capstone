"""
One-time setup: stores the Lakebase connection string as a Databricks secret
so pipeline/ingest_destinations.py (and later mcp_server/ and ui_app/) can
read it via dbutils.secrets.get(scope="trip-planner", key="lakebase-url")
without it ever being hardcoded or committed to git.

Run from a notebook cell in the Git folder:
    %sh python db/secret.py

If getpass doesn't work well in a %sh cell, use the Web Terminal instead
(available on Free Edition serverless compute).

Or locally, after `databricks auth login`:
    python db/secret.py
"""

import getpass
import subprocess
import sys

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "databricks-sdk"])
    from databricks.sdk import WorkspaceClient

SCOPE = "trip-planner"
KEY = "lakebase-url"


def main():
    w = WorkspaceClient()

    existing_scopes = [s.name for s in w.secrets.list_scopes()]
    if SCOPE not in existing_scopes:
        w.secrets.create_scope(scope=SCOPE)
        print(f"Created secret scope '{SCOPE}'")
    else:
        print(f"Secret scope '{SCOPE}' already exists")

    print(
        "Paste your Lakebase connection string — from the Lakebase project's "
        "'Connect' button -> Connection string tab -> a password-auth role."
    )
    conn_str = getpass.getpass("Lakebase connection string: ").strip()
    if not conn_str:
        raise SystemExit("No connection string entered, aborting.")

    w.secrets.put_secret(scope=SCOPE, key=KEY, string_value=conn_str)
    print(f"Stored as secret '{SCOPE}/{KEY}'")


if __name__ == "__main__":
    main()
