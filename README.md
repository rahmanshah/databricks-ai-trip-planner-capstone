# databricks-ai-trip-planner-capstone

An AI-powered trip and outdoor activity planner built on Databricks Free Edition. Users save destinations and activities, and an AI agent builds a weather-aware itinerary — rescheduling outdoor plans when rain or poor air quality is forecast, and explaining why.

Built for the [Databricks AI Bootcamp capstone](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) ("AI Trip and Outdoor Activity Planner" option), following patterns from three reference implementations — see [Acknowledgements](#acknowledgements).

## Status

Tracking progress phase by phase. Updated as each is completed.

- [x] **Phase 0** — Environment setup (Free Edition, LinkedIn verification, Lakebase instance, Git folder)
- [x] **Phase 1** — Lakebase schema
- [x] **Phase 2** — Spark ingestion pipeline (Open-Meteo + Wikimedia → Unity Catalog) — *verified end-to-end*
- [x] **Phase 3** — Embeddings + pgvector search in Lakebase — *verified end-to-end*
- [ ] **Phase 4** — MCP server + AI agent — *up next*
- [ ] **Phase 5** — Databricks App frontend
- [ ] **Phase 6** — Change data capture → Delta analytics
- [ ] **Phase 7** — End-to-end test + submission

## Architecture

Three Databricks Apps, one Lakebase project, one Unity Catalog schema:

```
Open-Meteo API ──┐                                    ┌── weather/geocoding tool calls (live)
                  ├─→ Spark pipeline (bronze/silver/gold, Unity Catalog)
Wikimedia API ────┘              │
                                  ▼
                    Embeddings ──→ Lakebase Postgres (pgvector)
                                        │        ▲
                                        ▼        │
                          mcp-trip-planner (MCP tool server) ──▶ agent-trip-planner (chat, exported from AI Playground)
                                        │
                                        ▼
                            trip-planner-ui (Flask + Lakebase, itinerary/packing CRUD)
                                        │
                                        ▼
                                   End users
```

Vector search lives **inside Lakebase** via the `pgvector` extension rather than a separate Databricks AI Search endpoint — proven to work on Free Edition, and avoids relying on a second preview-quality quota-limited feature alongside CDF.

## Capstone requirements → where they're met

| Requirement | Implementation |
|---|---|
| Data pipeline in Spark | `pipeline/ingest_destinations.py` — geocode, weather, air quality, Wikipedia → bronze/silver/gold Delta tables |
| Third-party API integration | Open-Meteo (geocoding, weather, air quality) + Wikimedia (destination descriptions) |
| Unstructured data processing | Destination descriptions, attraction blurbs, and user notes chunked and embedded (`pipeline/ingest_embeddings.py`) |
| Databricks App with a frontend | `trip-planner-ui` (itinerary/packing dashboard) + `agent-trip-planner` (chat) |
| AI agent with real read/write tools | `mcp_server/` — search, live conditions, generate itinerary, reschedule, packing list, edit itinerary |
| CDF from Lakebase into a Delta table | See [Known limitations](#known-limitations--free-edition-notes) — native Lakebase CDF is unreliable on Free Edition right now; using a Lakehouse Federation + scheduled MERGE fallback into a Delta table with native Delta CDF enabled |

## Third-party APIs

- **[Open-Meteo](https://open-meteo.com/)** — geocoding, weather forecast, air quality. No API key, no signup. Free tier: 10,000 calls/day, 5,000/hour, 600/minute, non-commercial use, CC BY 4.0 attribution required.
- **[Wikimedia REST API](https://www.mediawiki.org/wiki/API:Main_page)** — destination descriptions and nearby attractions. No API key, but every request needs a descriptive `User-Agent` header identifying this project, or requests may be throttled.

## Repo structure

```
db/
  schema.sql               # trips, destinations, activities, itinerary_items,
                            # weather_snapshots, packing_items, agent_actions,
                            # destination_embeddings (pgvector), cdc watermark table
  seed.sql
  secret.py                 # one-time: stores the Lakebase connection string as a Databricks secret
  grant_app_access.sql     # only needed if the schema-run role differs from the app's role
pipeline/
  ingest_destinations.py   # geocode + Open-Meteo + Wikimedia -> bronze/silver/gold (Unity Catalog)
  ingest_embeddings.py     # run as a notebook, not a standalone file (see notes below)
mcp_server/                # -> Databricks App "mcp-trip-planner"
  weather_broker.py
  wikimedia_broker.py
  lakebase_broker.py
  trip_mcp_server.py       # @mcp.tool functions only, thin wrappers over the brokers above
  app.yaml
  requirements.txt
ui_app/                    # -> Databricks App "trip-planner-ui"
  app.py                   # browser routes at /, scripted routes under /api/
  lakebase.py
  templates/
  setup_secrets.py
  app.yaml
  requirements.txt
check_environment.py       # Phase 0 sanity check: outbound reach + pgvector extension
test_sync.py                # smoke test using the notebook token-exchange auth pattern
```

`agent-trip-planner` (the chat agent) has no source folder here — it's generated by AI Playground's "Export to Databricks Apps" once `mcp-trip-planner`'s tools are wired up.

## Setup

1. [x] Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) and complete **LinkedIn verification** (unlocks outbound internet beyond the default trusted-domain allowlist — required to reach Open-Meteo and Wikimedia).
2. [x] Push this repo to GitHub, then in the Databricks workspace: **Workspace → Create → Git folder**, point it at the repo.
3. [x] **Catalog → Lakebase → Create Lakebase instance**. Once **Available**, under **Roles & Databases**, create a password-auth role and copy the connection string.
4. [x] Run `db/schema.sql` in the Lakebase SQL editor, then `db/seed.sql`.
5. [x] Run `%sh python db/secret.py` from a notebook cell — prompts (via `getpass`) for the Lakebase connection string from step 3 and stores it as a Databricks secret (`trip-planner`/`lakebase-url`). If `getpass` misbehaves in a `%sh` cell, use the Web Terminal instead.
6. [x] Edit the `WIKIMEDIA_USER_AGENT` placeholder in both `check_environment.py` and `pipeline/ingest_destinations.py` — Wikimedia's API etiquette requires a real contact, not a placeholder or `example.com` address.
7. [x] Run `check_environment.py` (`%sh python check_environment.py`) — confirms outbound reach to Open-Meteo (geocoding/forecast/air-quality) and Wikimedia, that the User-Agent was actually edited, and that the Lakebase connection + `pgvector` extension both work. Fix any `[FAIL]` line before moving on.
8. [x] Run `pipeline/ingest_destinations.py` as an actual notebook (not the file-editor "Run" button — it creates the `trip_planner` catalog/schemas/tables itself on first run, no manual pre-creation needed).

## Known limitations / Free Edition notes

Learned the hard way across the reference repos — documenting up front so they don't cost time twice:

- **Outbound internet is allowlisted by default.** LinkedIn verification is required before any external API call will succeed from serverless compute.
- **`psycopg2-binary` can abort the entire Python kernel (SIGABRT, exit code 134) on Free Edition serverless notebooks** — same failure class as other packages that bundle compiled native extensions (`cv2`, `pymssql`) hitting missing/mismatched shared libraries in the container. Switched to `pg8000` (pure Python, no compiled extensions) for all Lakebase connections in notebooks; it takes keyword args rather than a `postgresql://` URL, so connection helpers parse the URL manually.
- **Postgres UUID columns come back as Python `uuid.UUID` objects**, not strings — from both `pg8000` and `psycopg2`. Spark's `createDataFrame()` schema inference has no idea what to do with a raw `uuid.UUID` object and fails outright. Cast to `str()` at the point each id is read from the database, before it flows into anything Spark-bound.
- **Spark's `createDataFrame()` schema inference from a list of Python dicts is fragile** — it fails if a field's value is `None` across every sampled row (can't infer *any* type) or if the same field is `int` in some rows and `float` in others (e.g. a JSON API returning `1200` vs `1234.5` for the same logical field). Give explicit `StructType` schemas to any DataFrame built from external API data rather than relying on inference — this class of bug hit `destination_id`, `distance_m`, and `air_quality_index` in `pipeline/ingest_destinations.py` before all `createDataFrame()` calls there were pinned to explicit schemas.
- **Open-Meteo's "16-day forecast" counts today as day one** — the furthest valid `end_date` is `today + 15`, not `today + 16`. Off by one here gets a `400` with a `reason` field naming the actual allowed range.
- **Open-Meteo's air-quality API has its own, shorter forecast horizon that isn't documented alongside the main weather API's** and can drift day to day — don't hardcode a day count for it. `fetch_air_quality_safe()` in `pipeline/ingest_destinations.py` parses the allowed range straight out of the API's own error message and retries once with the corrected window, falling back to weather-without-AQI rather than losing the whole row if it still can't get air-quality data.
- **Always capture the response body on 4xx errors from Open-Meteo/Wikimedia**, not just the status code — both return a JSON `{"error": true, "reason": "..."}` body that names the exact problem, which `raise_for_status()` alone discards.
- **Lakebase's native Change Data Feed is Public Preview and currently unreliable on Free Edition** — there are open reports of the destination Delta table never appearing after a successful-looking setup. Plan is to fall back to registering Lakebase as a read-only Unity Catalog foreign catalog and running a scheduled incremental `MERGE` into a Delta table with native `delta.enableChangeDataFeed = true`, rather than depending on the Lakebase-side preview.
- **Databricks Apps only allow programmatic (Bearer-token) access under `/api/`** — routes outside that prefix expect a browser session and return 401 to scripts/curl.
- **Calling a deployed app from a notebook needs a token-exchange step**, not a plain `WorkspaceClient().config.authenticate()` call — that only works for service-principal/app-to-app callers.
- **Each standalone file "Run" gets a fresh, ephemeral serverless environment.** Packages installed by one script run don't persist into the next — scripts that need dependencies should self-install them at the top of their own execution.
- **`torch`/`sentence-transformers` should be installed via a notebook's Environment side panel**, not mid-script `pip install` — doing it mid-script has caused kernel crashes. `pipeline/ingest_embeddings.py` must be run as an actual notebook, not the file-editor "Run" button.
- **No relevance floor on search results — flagged for Phase 4.** With only a handful of chunks in the index so far, pgvector's `<=>` always returns *something* as the top match regardless of whether it's actually relevant. Confirmed while testing `search_destinations()` at the end of `pipeline/ingest_embeddings.py`: a query about a coastal city with hiking nearby returned a Mount Rainier clouds/rain chunk as the top result at only 21% similarity — mechanically correct, semantically wrong. Same gap the `weather-intelligence` reference repo hit (they used ~40% as a rough cutoff). The Phase 4 MCP `search_destinations` tool should treat low-similarity results as "no good match" rather than confidently returning the top-k regardless.
- **Registering an MCP server as a Unity Catalog "MCP Service" via AI Gateway fails with 401 on Free Edition** (the Managed MCP Servers preview isn't enabled). Use the **Custom MCP Server** tool picker in AI Playground/Agent Bricks instead — any deployed app named `mcp-*` is auto-discovered there with no extra registration step.
- **Grant table access explicitly** if the Postgres role that ran `db/schema.sql` differs from the role the deployed app connects with (`db/grant_app_access.sql`).

## Acknowledgements

Structure and Free Edition patterns adapted from:

- [EcZachly/databricks-ai-bootcamp-capstone](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) — the capstone brief this project fulfills
- [rahmanshah/databricks-lakebase-app-weather-intelligence](https://github.com/rahmanshah/databricks-lakebase-app-weather-intelligence) — pgvector-in-Lakebase pattern
- [rahmanshah/databricks-weather-mcp-agent](https://github.com/rahmanshah/databricks-weather-mcp-agent) — MCP server + Export-to-Apps agent pattern
- [rahmanshah/databricks-lakebase-app-ticketing-system](https://github.com/rahmanshah/databricks-lakebase-app-ticketing-system) — Flask + Lakebase CRUD app pattern