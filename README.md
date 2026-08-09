# databricks-ai-trip-planner-capstone

An AI-powered trip and outdoor activity planner built on Databricks Free Edition. Users save destinations and activities, and an AI agent builds a weather-aware itinerary — rescheduling outdoor plans when rain or poor air quality is forecast, and explaining why.

Built for the [Databricks AI Bootcamp capstone](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) ("AI Trip and Outdoor Activity Planner" option), following patterns from three reference implementations — see [Acknowledgements](#acknowledgements).

## Status

Tracking progress phase by phase. Updated as each is completed.

- [ ] **Phase 0** — Environment setup (Free Edition, LinkedIn verification, Lakebase instance, Git folder) — *in progress*
- [ ] **Phase 1** — Lakebase schema
- [ ] **Phase 2** — Spark ingestion pipeline (Open-Meteo + Wikimedia → Unity Catalog)
- [ ] **Phase 3** — Embeddings + pgvector search in Lakebase
- [ ] **Phase 4** — MCP server + AI agent
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

1. [ ] Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) and complete **LinkedIn verification** (unlocks outbound internet beyond the default trusted-domain allowlist — required to reach Open-Meteo and Wikimedia).
2. [ ] Push this repo to GitHub, then in the Databricks workspace: **Workspace → Create → Git folder**, point it at the repo.
3. [ ] **Catalog → Lakebase → Create Lakebase instance**. Once **Available**, under **Roles & Databases**, create a password-auth role and copy the connection string.
4. [ ] Run `check_environment.py` (`%sh python check_environment.py` from a notebook cell in the Git folder) to confirm serverless compute can reach `open-meteo.com` and `*.wikimedia.org`, and that `CREATE EXTENSION vector;` works on the Lakebase instance.
5. [ ] Create a Unity Catalog catalog/schema for the pipeline, e.g. `trip_planner.bronze` / `.silver` / `.gold`.

## Known limitations / Free Edition notes

Learned the hard way across the reference repos — documenting up front so they don't cost time twice:

- **Outbound internet is allowlisted by default.** LinkedIn verification is required before any external API call will succeed from serverless compute.
- **Lakebase's native Change Data Feed is Public Preview and currently unreliable on Free Edition** — there are open reports of the destination Delta table never appearing after a successful-looking setup. Plan is to fall back to registering Lakebase as a read-only Unity Catalog foreign catalog and running a scheduled incremental `MERGE` into a Delta table with native `delta.enableChangeDataFeed = true`, rather than depending on the Lakebase-side preview.
- **Databricks Apps only allow programmatic (Bearer-token) access under `/api/`** — routes outside that prefix expect a browser session and return 401 to scripts/curl.
- **Calling a deployed app from a notebook needs a token-exchange step**, not a plain `WorkspaceClient().config.authenticate()` call — that only works for service-principal/app-to-app callers.
- **Each standalone file "Run" gets a fresh, ephemeral serverless environment.** Packages installed by one script run don't persist into the next — scripts that need dependencies should self-install them at the top of their own execution.
- **`torch`/`sentence-transformers` should be installed via a notebook's Environment side panel**, not mid-script `pip install` — doing it mid-script has caused kernel crashes. `pipeline/ingest_embeddings.py` must be run as an actual notebook, not the file-editor "Run" button.
- **Registering an MCP server as a Unity Catalog "MCP Service" via AI Gateway fails with 401 on Free Edition** (the Managed MCP Servers preview isn't enabled). Use the **Custom MCP Server** tool picker in AI Playground/Agent Bricks instead — any deployed app named `mcp-*` is auto-discovered there with no extra registration step.
- **Grant table access explicitly** if the Postgres role that ran `db/schema.sql` differs from the role the deployed app connects with (`db/grant_app_access.sql`).

## Acknowledgements

Structure and Free Edition patterns adapted from:

- [EcZachly/databricks-ai-bootcamp-capstone](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) — the capstone brief this project fulfills
- [rahmanshah/databricks-lakebase-app-weather-intelligence](https://github.com/rahmanshah/databricks-lakebase-app-weather-intelligence) — pgvector-in-Lakebase pattern
- [rahmanshah/databricks-weather-mcp-agent](https://github.com/rahmanshah/databricks-weather-mcp-agent) — MCP server + Export-to-Apps agent pattern
- [rahmanshah/databricks-lakebase-app-ticketing-system](https://github.com/rahmanshah/databricks-lakebase-app-ticketing-system) — Flask + Lakebase CRUD app pattern
