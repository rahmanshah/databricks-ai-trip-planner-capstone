"""
Open-Meteo HTTP calls for the MCP server's live weather tools.

This is deliberately separate from pipeline/ingest_destinations.py's fetch
functions — that one does date-range batch backfills into Delta/Lakebase,
this one does single-date, on-demand lookups at agent runtime. Same API,
same lessons learned (off-by-one forecast horizon, independent/shorter air
quality horizon, capture the error body), different shape of call.
"""

import requests
from datetime import date, timedelta

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Open-Meteo's "16-day forecast" counts today as day one, so the furthest
# valid end_date is today+15, not today+16 (see README Known limitations).
MAX_FORECAST_DAYS = 15

# Thresholds for turning raw numbers into a plain-language, explainable
# recommendation — this is what satisfies the capstone's "explain why it
# made each weather-based change" requirement. Centralized here so every
# tool that reasons about weather (get_live_conditions, generate_itinerary,
# reschedule_activity) uses the same thresholds and the same wording.
RAIN_RESCHEDULE_THRESHOLD_PCT = 60
UMBRELLA_THRESHOLD_PCT = 40
AQI_RESCHEDULE_THRESHOLD = 100
COLD_THRESHOLD_C = 10


def _raise_with_body(resp):
    if not resp.ok:
        raise requests.HTTPError(f"{resp.status_code} for {resp.url}: {resp.text[:500]}")


def fetch_forecast(lat, lon, target_date):
    """Single-day forecast. Returns (forecast_dict, None) on success or
    (None, human_readable_reason) if the date is out of range — callers
    can surface that reason directly rather than getting a raw exception."""
    today = date.today()
    max_date = today + timedelta(days=MAX_FORECAST_DAYS)
    if target_date > max_date:
        return None, (
            f"{target_date.isoformat()} is beyond Open-Meteo's forecast horizon "
            f"(furthest available: {max_date.isoformat()})."
        )
    if target_date < today:
        return None, f"{target_date.isoformat()} is in the past — no forecast available."

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
        "timezone": "auto",
    }
    resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=10)
    _raise_with_body(resp)
    daily = resp.json().get("daily", {})
    if not daily.get("time"):
        return None, "No forecast data returned for that date."
    return {
        "temperature_high_c": daily["temperature_2m_max"][0],
        "temperature_low_c": daily["temperature_2m_min"][0],
        "precipitation_probability_pct": daily["precipitation_probability_max"][0],
        "weather_code": daily["weathercode"][0],
    }, None


def fetch_air_quality(lat, lon, target_date):
    """Best-effort, returns None rather than raising on failure — air
    quality's forecast horizon is shorter than and independent of the main
    weather API's, so this can legitimately be unavailable even when
    fetch_forecast succeeds for the same date (see README)."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "us_aqi",
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
        "timezone": "auto",
    }
    try:
        resp = requests.get(OPEN_METEO_AIR_QUALITY_URL, params=params, timeout=10)
        _raise_with_body(resp)
    except requests.HTTPError:
        return None
    values = [v for v in resp.json().get("hourly", {}).get("us_aqi", []) if v is not None]
    return round(sum(values) / len(values)) if values else None


def get_conditions_with_recommendation(lat, lon, target_date):
    """The actual tool logic behind get_live_conditions: raw forecast plus
    an explicit, threshold-based recommendation and the reasoning behind
    it — not just a number dump the agent has to interpret on its own."""
    forecast, error = fetch_forecast(lat, lon, target_date)
    if forecast is None:
        return {"available": False, "reason": error}

    forecast["air_quality_index"] = fetch_air_quality(lat, lon, target_date)

    precip = forecast["precipitation_probability_pct"]
    low = forecast["temperature_low_c"]
    aqi = forecast["air_quality_index"]

    reasons = []
    should_reschedule = False

    if precip is not None and precip >= RAIN_RESCHEDULE_THRESHOLD_PCT:
        should_reschedule = True
        reasons.append(
            f"{precip}% chance of rain, at or above the {RAIN_RESCHEDULE_THRESHOLD_PCT}% "
            "threshold for outdoor activities"
        )
    elif precip is not None and precip >= UMBRELLA_THRESHOLD_PCT:
        reasons.append(f"{precip}% chance of rain — worth bringing an umbrella")

    if aqi is not None and aqi >= AQI_RESCHEDULE_THRESHOLD:
        should_reschedule = True
        reasons.append(
            f"air quality index {aqi}, at or above the {AQI_RESCHEDULE_THRESHOLD} "
            "threshold considered unhealthy for extended outdoor exposure"
        )

    if low is not None and low <= COLD_THRESHOLD_C:
        reasons.append(f"low of {low}°C — pack warm layers")

    if not reasons:
        reasons.append("conditions look fine for outdoor plans")

    return {
        "available": True,
        "forecast": forecast,
        "should_reschedule_outdoor_activities": should_reschedule,
        "reasoning": " ".join(reasons),
    }
