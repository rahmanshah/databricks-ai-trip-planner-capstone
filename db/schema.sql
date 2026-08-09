-- ============================================================================
-- Trip Planner Capstone — Lakebase Postgres schema
-- Run this in the Lakebase SQL editor, connected to your instance.
-- Idempotent: safe to re-run (CREATE ... IF NOT EXISTS everywhere).
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

-- ----------------------------------------------------------------------------
-- updated_at trigger helper
-- Every table that the CDC fallback pipeline reads incrementally (see
-- README "Known limitations") gets updated_at + is_deleted, since the
-- fallback watermarks on updated_at instead of native row-level CDC.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- users
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name TEXT NOT NULL,
    email        TEXT UNIQUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- trips
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id   UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title      TEXT NOT NULL,
    start_date DATE,
    end_date   DATE,
    status     TEXT NOT NULL DEFAULT 'planning'
                 CHECK (status IN ('planning','confirmed','completed','cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_trips_owner ON trips(owner_id);

DROP TRIGGER IF EXISTS trg_trips_updated_at ON trips;
CREATE TRIGGER trg_trips_updated_at
    BEFORE UPDATE ON trips
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ----------------------------------------------------------------------------
-- destinations
-- description is populated from the Wikimedia summary during the Spark
-- pipeline's silver/gold step (Phase 2), not typed in by users.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS destinations (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id        UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    description    TEXT,
    arrival_date   DATE,
    departure_date DATE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted     BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_destinations_trip ON destinations(trip_id);

DROP TRIGGER IF EXISTS trg_destinations_updated_at ON destinations;
CREATE TRIGGER trg_destinations_updated_at
    BEFORE UPDATE ON destinations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ----------------------------------------------------------------------------
-- activities
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS activities (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    destination_id   UUID NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    category         TEXT NOT NULL DEFAULT 'other'
                        CHECK (category IN ('hiking','sightseeing','museum','dining','water_sports','other')),
    is_outdoor       BOOLEAN NOT NULL DEFAULT true,
    description      TEXT,
    duration_minutes INTEGER,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted       BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_activities_destination ON activities(destination_id);

DROP TRIGGER IF EXISTS trg_activities_updated_at ON activities;
CREATE TRIGGER trg_activities_updated_at
    BEFORE UPDATE ON activities
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ----------------------------------------------------------------------------
-- itinerary_items
-- reschedule_reason holds the agent's explanation when status = 'rescheduled'
-- (satisfies "explain why it made each weather-based change").
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS itinerary_items (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id            UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    activity_id        UUID NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    scheduled_date     DATE NOT NULL,
    scheduled_time     TIME,
    position           INTEGER NOT NULL DEFAULT 0,
    status             TEXT NOT NULL DEFAULT 'planned'
                          CHECK (status IN ('planned','rescheduled','completed','cancelled')),
    reschedule_reason  TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted         BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_itinerary_items_trip ON itinerary_items(trip_id);
CREATE INDEX IF NOT EXISTS idx_itinerary_items_activity ON itinerary_items(activity_id);

DROP TRIGGER IF EXISTS trg_itinerary_items_updated_at ON itinerary_items;
CREATE TRIGGER trg_itinerary_items_updated_at
    BEFORE UPDATE ON itinerary_items
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ----------------------------------------------------------------------------
-- weather_snapshots
-- Populated both by the batch Spark pipeline and by the agent's live
-- get_live_conditions tool; upsert on the unique key keeps one row per
-- destination/date/source.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_snapshots (
    id                             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    destination_id                 UUID NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    forecast_date                  DATE NOT NULL,
    temperature_high_c             DOUBLE PRECISION,
    temperature_low_c              DOUBLE PRECISION,
    precipitation_probability_pct  INTEGER,
    air_quality_index              INTEGER,
    weather_code                   INTEGER,
    source                         TEXT NOT NULL DEFAULT 'open-meteo',
    fetched_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (destination_id, forecast_date, source)
);
CREATE INDEX IF NOT EXISTS idx_weather_snapshots_destination ON weather_snapshots(destination_id);

-- ----------------------------------------------------------------------------
-- packing_items
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS packing_items (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id    UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    item_name  TEXT NOT NULL,
    category   TEXT NOT NULL DEFAULT 'general'
                 CHECK (category IN ('clothing','gear','documents','general')),
    is_packed  BOOLEAN NOT NULL DEFAULT false,
    added_by   TEXT NOT NULL DEFAULT 'agent' CHECK (added_by IN ('agent','user')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_packing_items_trip ON packing_items(trip_id);

DROP TRIGGER IF EXISTS trg_packing_items_updated_at ON packing_items;
CREATE TRIGGER trg_packing_items_updated_at
    BEFORE UPDATE ON packing_items
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ----------------------------------------------------------------------------
-- agent_actions
-- Append-only audit log of every MCP tool call — this is what makes the
-- CDF/analytics requirement meaningful instead of an empty table.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_actions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id     UUID REFERENCES trips(id) ON DELETE SET NULL,
    tool_name   TEXT NOT NULL,
    tool_input  JSONB NOT NULL,
    tool_output JSONB,
    status      TEXT NOT NULL DEFAULT 'success' CHECK (status IN ('success','error')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_agent_actions_trip ON agent_actions(trip_id);
CREATE INDEX IF NOT EXISTS idx_agent_actions_tool ON agent_actions(tool_name);

-- ----------------------------------------------------------------------------
-- destination_embeddings (pgvector)
-- model_name stored per row so re-embedding with a different model later
-- doesn't require a schema change or lose track of what generated each vector.
-- 384 dims matches sentence-transformers/all-MiniLM-L6-v2.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS destination_embeddings (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    destination_id UUID NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    chunk_index    INTEGER NOT NULL DEFAULT 0,
    chunk_text     TEXT NOT NULL,
    embedding      vector(384) NOT NULL,
    model_name     TEXT NOT NULL DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (destination_id, chunk_index, model_name)
);
CREATE INDEX IF NOT EXISTS idx_destination_embeddings_hnsw
    ON destination_embeddings USING hnsw (embedding vector_cosine_ops);

-- ----------------------------------------------------------------------------
-- cdc_watermarks
-- Tracks progress for the Lakehouse Federation + scheduled MERGE fallback
-- (README "Known limitations") — one row per source table being replicated
-- into Delta. last_synced_at starts at epoch so the first run pulls everything.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cdc_watermarks (
    table_name     TEXT PRIMARY KEY,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'::timestamptz
);
INSERT INTO cdc_watermarks (table_name) VALUES
    ('trips'), ('activities'), ('itinerary_items'), ('packing_items'), ('agent_actions')
ON CONFLICT (table_name) DO NOTHING;

-- ----------------------------------------------------------------------------
-- Optional: native Lakebase CDF (Public Preview)
-- Worth a quick test before relying on the fallback above — uncomment and
-- run, then start CDF from the Lakebase UI's Branch overview -> CDF tab.
-- ----------------------------------------------------------------------------
-- ALTER TABLE trips            REPLICA IDENTITY FULL;
-- ALTER TABLE itinerary_items  REPLICA IDENTITY FULL;
-- ALTER TABLE packing_items    REPLICA IDENTITY FULL;
-- ALTER TABLE agent_actions    REPLICA IDENTITY FULL;

-- ----------------------------------------------------------------------------
-- Sanity check — run after applying the schema above.
-- ----------------------------------------------------------------------------
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'public' ORDER BY table_name;
