-- ============================================================================
-- Trip Planner Capstone — seed data
-- Run after schema.sql. Idempotent via fixed UUIDs + ON CONFLICT DO NOTHING,
-- so re-running this is safe.
--
-- Dates are relative to CURRENT_DATE (not hardcoded) so the trip always sits
-- in the near future — useful later for testing the agent's live weather
-- tool against Open-Meteo's real forecast window.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- users
-- ----------------------------------------------------------------------------
INSERT INTO users (id, display_name, email) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Test User', 'test.user@example.com')
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- trips
-- ----------------------------------------------------------------------------
INSERT INTO trips (id, owner_id, title, start_date, end_date, status) VALUES
    ('22222222-2222-2222-2222-222222222222',
     '11111111-1111-1111-1111-111111111111',
     'Pacific Northwest Hiking Trip',
     CURRENT_DATE + INTERVAL '14 days',
     CURRENT_DATE + INTERVAL '20 days',
     'planning')
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- destinations
-- description is a placeholder here — Phase 2's Spark pipeline overwrites it
-- with the real Wikimedia summary once it runs.
-- ----------------------------------------------------------------------------
INSERT INTO destinations (id, trip_id, name, latitude, longitude, description, arrival_date, departure_date) VALUES
    ('33333333-3333-3333-3333-333333333331',
     '22222222-2222-2222-2222-222222222222',
     'Seattle, WA', 47.6062, -122.3321,
     'Placeholder description — replaced by the Wikimedia ingestion pipeline.',
     CURRENT_DATE + INTERVAL '14 days', CURRENT_DATE + INTERVAL '16 days'),
    ('33333333-3333-3333-3333-333333333332',
     '22222222-2222-2222-2222-222222222222',
     'Mount Rainier National Park', 46.8523, -121.7603,
     'Placeholder description — replaced by the Wikimedia ingestion pipeline.',
     CURRENT_DATE + INTERVAL '16 days', CURRENT_DATE + INTERVAL '20 days')
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- activities
-- ----------------------------------------------------------------------------
INSERT INTO activities (id, destination_id, name, category, is_outdoor, description, duration_minutes) VALUES
    ('44444444-4444-4444-4444-444444444441',
     '33333333-3333-3333-3333-333333333331',
     'Pike Place Market visit', 'sightseeing', false,
     'Browse the market stalls and grab lunch.', 90),
    ('44444444-4444-4444-4444-444444444442',
     '33333333-3333-3333-3333-333333333331',
     'Kerry Park viewpoint', 'sightseeing', true,
     'Skyline photo stop overlooking downtown Seattle.', 45),
    ('44444444-4444-4444-4444-444444444443',
     '33333333-3333-3333-3333-333333333332',
     'Skyline Trail hike', 'hiking', true,
     'Loop trail with wildflower meadows and mountain views.', 180),
    ('44444444-4444-4444-4444-444444444444',
     '33333333-3333-3333-3333-333333333332',
     'Paradise Visitor Center', 'museum', false,
     'Indoor exhibits and trip planning desk — good fallback if weather turns.', 60)
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- itinerary_items
-- One activity per day across the trip. All 'planned' — none rescheduled yet
-- since there's no agent running against real forecasts here.
-- ----------------------------------------------------------------------------
INSERT INTO itinerary_items (id, trip_id, activity_id, scheduled_date, scheduled_time, position, status) VALUES
    ('55555555-5555-5555-5555-555555555551',
     '22222222-2222-2222-2222-222222222222',
     '44444444-4444-4444-4444-444444444441',
     CURRENT_DATE + INTERVAL '14 days', '10:00', 1, 'planned'),
    ('55555555-5555-5555-5555-555555555552',
     '22222222-2222-2222-2222-222222222222',
     '44444444-4444-4444-4444-444444444442',
     CURRENT_DATE + INTERVAL '15 days', '17:00', 1, 'planned'),
    ('55555555-5555-5555-5555-555555555553',
     '22222222-2222-2222-2222-222222222222',
     '44444444-4444-4444-4444-444444444443',
     CURRENT_DATE + INTERVAL '17 days', '09:00', 1, 'planned'),
    ('55555555-5555-5555-5555-555555555554',
     '22222222-2222-2222-2222-222222222222',
     '44444444-4444-4444-4444-444444444444',
     CURRENT_DATE + INTERVAL '18 days', '13:00', 1, 'planned')
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- weather_snapshots
-- Static placeholder numbers — the real pipeline/live tool overwrites these
-- via the UNIQUE (destination_id, forecast_date, source) upsert key. One row
-- deliberately has a high rain probability so there's something for the
-- agent's rescheduling logic to react to once it exists.
-- ----------------------------------------------------------------------------
INSERT INTO weather_snapshots (destination_id, forecast_date, temperature_high_c, temperature_low_c, precipitation_probability_pct, air_quality_index, weather_code, source) VALUES
    ('33333333-3333-3333-3333-333333333331', CURRENT_DATE + INTERVAL '14 days', 22, 14, 10, 35, 1, 'open-meteo'),
    ('33333333-3333-3333-3333-333333333331', CURRENT_DATE + INTERVAL '15 days', 19, 13, 65, 40, 61, 'open-meteo'),
    ('33333333-3333-3333-3333-333333333332', CURRENT_DATE + INTERVAL '17 days', 18, 8,  20, 25, 2, 'open-meteo'),
    ('33333333-3333-3333-3333-333333333332', CURRENT_DATE + INTERVAL '18 days', 15, 6,  70, 30, 63, 'open-meteo')
ON CONFLICT (destination_id, forecast_date, source) DO NOTHING;

-- ----------------------------------------------------------------------------
-- packing_items
-- ----------------------------------------------------------------------------
INSERT INTO packing_items (id, trip_id, item_name, category, is_packed, added_by) VALUES
    ('66666666-6666-6666-6666-666666666661', '22222222-2222-2222-2222-222222222222', 'Hiking boots', 'gear', false, 'agent'),
    ('66666666-6666-6666-6666-666666666662', '22222222-2222-2222-2222-222222222222', 'Rain jacket', 'clothing', false, 'agent'),
    ('66666666-6666-6666-6666-666666666663', '22222222-2222-2222-2222-222222222222', 'Passport', 'documents', true, 'user'),
    ('66666666-6666-6666-6666-666666666664', '22222222-2222-2222-2222-222222222222', 'Refillable water bottle', 'gear', false, 'user')
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- agent_actions
-- Sample log rows in the shape the real MCP tools will write, so the
-- Phase 6 CDC fallback pipeline has something to sync before the agent
-- exists.
-- ----------------------------------------------------------------------------
INSERT INTO agent_actions (id, trip_id, tool_name, tool_input, tool_output, status) VALUES
    ('77777777-7777-7777-7777-777777777771',
     '22222222-2222-2222-2222-222222222222',
     'search_destinations',
     '{"query": "relaxed coastal town with good hiking nearby"}'::jsonb,
     '{"results": ["Seattle, WA", "Mount Rainier National Park"]}'::jsonb,
     'success'),
    ('77777777-7777-7777-7777-777777777772',
     '22222222-2222-2222-2222-222222222222',
     'build_packing_list',
     '{"trip_id": "22222222-2222-2222-2222-222222222222"}'::jsonb,
     '{"items_added": 4}'::jsonb,
     'success')
ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- Sanity check — run after seeding.
-- ----------------------------------------------------------------------------
-- SELECT 'users' AS table_name, count(*) FROM users
-- UNION ALL SELECT 'trips', count(*) FROM trips
-- UNION ALL SELECT 'destinations', count(*) FROM destinations
-- UNION ALL SELECT 'activities', count(*) FROM activities
-- UNION ALL SELECT 'itinerary_items', count(*) FROM itinerary_items
-- UNION ALL SELECT 'weather_snapshots', count(*) FROM weather_snapshots
-- UNION ALL SELECT 'packing_items', count(*) FROM packing_items
-- UNION ALL SELECT 'agent_actions', count(*) FROM agent_actions
-- ORDER BY table_name;
