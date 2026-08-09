"""
Trip Planner MCP server — deploy this as a Databricks App named
mcp-trip-planner (the mcp- prefix is required for auto-discovery in AI
Playground's Custom MCP Server picker).

This file is the thin layer: it declares the 6 @mcp.tool functions and
combines weather_broker.py (live Open-Meteo calls) with lakebase_broker.py
(Lakebase reads/writes). Scheduling decisions — which day to put an
activity on, which day to move a rescheduled one to — live here rather than
in lakebase_broker.py, which stays a pure data layer.
"""

import os
from datetime import date, timedelta

from mcp.server.fastmcp import FastMCP

import lakebase_broker
import weather_broker

mcp = FastMCP(
    "trip-planner",
    host="0.0.0.0",
    port=int(os.environ.get("DATABRICKS_APP_PORT", 8000)),
)

# Load the embedding model now, at startup, rather than lazily on the first
# search_destinations call. Lazy loading meant the first real call paid the
# one-time cost of downloading/initializing sentence-transformers inline —
# in testing this took long enough to trip a stream timeout (RST_STREAM /
# INTERNAL_ERROR), with the retry succeeding fast once the model was
# already in memory. Loading it here means that cost happens once during
# app startup, not during a user's first search.
lakebase_broker.get_embedding_model()


# ---------------------------------------------------------------------------
# Scheduling helpers (not tools — internal to generate_itinerary / reschedule_activity)
# ---------------------------------------------------------------------------

def _rank_days_by_weather(destination):
    """Destination's arrival->departure days, best-weather-first (lowest
    cached precipitation probability). Falls back to arrival->departure
    order if no weather is cached for those dates."""
    start, end = destination["arrival_date"], destination["departure_date"]
    if not start or not end or end < start:
        return [d for d in [start, end] if d]
    all_days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    precip_by_day = {
        w["forecast_date"]: w["precipitation_probability_pct"]
        for w in destination.get("weather", [])
        if w["precipitation_probability_pct"] is not None
    }
    return sorted(all_days, key=lambda d: precip_by_day.get(d, 50))  # unknown days treated as neutral


def _build_itinerary_for_destination(destination, existing_activity_ids):
    """Assigns dates to this destination's not-yet-scheduled activities.
    Outdoor activities get first pick of the best-weather days when
    weather is cached; indoor activities fill in the rest."""
    unscheduled = [a for a in destination["activities"] if a["id"] not in existing_activity_ids]
    if not unscheduled:
        return [], []

    ranked_days = _rank_days_by_weather(destination)
    if not ranked_days:
        return [], [f"{destination['name']}: no arrival/departure dates set, skipped scheduling its activities."]

    outdoor = [a for a in unscheduled if a["is_outdoor"]]
    indoor = [a for a in unscheduled if not a["is_outdoor"]]
    ordered = outdoor + indoor

    new_items = []
    day_load = {}
    for i, activity in enumerate(ordered):
        day = ranked_days[i % len(ranked_days)]
        day_load[day] = day_load.get(day, 0) + 1
        new_items.append({
            "activity_id": activity["id"],
            "scheduled_date": day,
            "position": day_load[day],
            "status": "planned",
        })

    note = f"{destination['name']}: scheduled {len(new_items)} activities across {len(ranked_days)} day(s)"
    if outdoor and destination.get("weather"):
        note += ", outdoor activities prioritized on lower-precipitation days"
    return new_items, [note]


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def health() -> dict:
    """Diagnostic tool confirming the server is running and can reach Lakebase."""
    try:
        lakebase_broker.get_lakebase_connection().close()
        lakebase_ok = True
    except Exception:
        lakebase_ok = False
    return {"status": "ok", "lakebase_reachable": lakebase_ok}


@mcp.tool()
def search_destinations(query: str, top_k: int = 5) -> dict:
    """Semantically search this trip's saved destinations and their nearby
    attractions. USE FOR: finding which saved destination best matches a
    vague or descriptive request (e.g. "somewhere with good hiking and mild
    weather"). Returns matches ranked by relevance, or an explicit no-match
    message if nothing in the index is a strong enough semantic match —
    treat that message as a real answer, not an error to work around.
    """
    return lakebase_broker.search_destinations(query, top_k=top_k)


@mcp.tool()
def get_live_conditions(destination_id: str, target_date: str) -> dict:
    """Look up live weather, air quality, and an explicit reschedule
    recommendation with reasoning for one destination on one date. USE FOR:
    checking whether a day's forecast is good for outdoor plans, or
    explaining why an activity should move. destination_id must be a real
    id already returned by search_destinations or otherwise known from
    context — never invent, guess, or use placeholder text for it; if you
    don't have a real id yet, call search_destinations first. target_date
    must be an ISO date string (YYYY-MM-DD) within roughly the next 15
    days — Open-Meteo's forecast horizon.
    """
    destination = lakebase_broker.get_destination(destination_id)
    if not destination:
        return {"available": False, "reason": f"No destination found with id {destination_id}."}
    if destination["latitude"] is None or destination["longitude"] is None:
        return {"available": False, "reason": f"{destination['name']} has no coordinates yet — run the ingestion pipeline first."}

    parsed_date = date.fromisoformat(target_date)
    result = weather_broker.get_conditions_with_recommendation(
        destination["latitude"], destination["longitude"], parsed_date
    )
    result["destination_name"] = destination["name"]
    return result


@mcp.tool()
def get_itinerary(trip_id: str) -> dict:
    """Read the full current itinerary for a trip — every scheduled item
    with its activity name, destination, date, and status. USE FOR:
    answering "what's on my itinerary" questions, and — importantly —
    looking up a real itinerary_item_id before calling reschedule_activity
    or move_or_remove_itinerary_item. Those ids aren't guessable from
    conversation (e.g. "the hike" or "Tuesday's museum visit") — call this
    first to find the matching id from context.
    """
    overview = lakebase_broker.get_trip_overview(trip_id)
    if not overview:
        return {"success": False, "reason": f"No trip found with id {trip_id}."}

    activity_by_id = {
        a["id"]: {"name": a["name"], "destination": d["name"]}
        for d in overview["destinations"] for a in d["activities"]
    }

    items = [
        {
            "itinerary_item_id": item["id"],
            "activity_name": activity_by_id.get(item["activity_id"], {}).get("name"),
            "destination_name": activity_by_id.get(item["activity_id"], {}).get("destination"),
            "scheduled_date": item["scheduled_date"].isoformat() if item["scheduled_date"] else None,
            "status": item["status"],
            "reschedule_reason": item["reschedule_reason"],
        }
        for item in overview["itinerary_items"]
    ]
    return {"success": True, "trip_title": overview["trip"]["title"], "items": items}


@mcp.tool()
def generate_itinerary(trip_id: str) -> dict:
    """Build or extend the day-by-day itinerary for a trip by scheduling
    any activities not on it yet. USE FOR: creating an initial plan, or
    filling in newly-added activities. Distributes activities across each
    destination's arrival/departure window, and when cached weather is
    available, schedules outdoor activities on the lowest-precipitation
    days first. Never touches activities already on the itinerary — use
    reschedule_activity or move_or_remove_itinerary_item for those.
    """
    overview = lakebase_broker.get_trip_overview(trip_id)
    if not overview:
        return {"success": False, "reason": f"No trip found with id {trip_id}."}

    existing_activity_ids = {item["activity_id"] for item in overview["itinerary_items"]}

    all_new_items, all_notes = [], []
    for destination in overview["destinations"]:
        new_items, notes = _build_itinerary_for_destination(destination, existing_activity_ids)
        for item in new_items:
            item["trip_id"] = trip_id
        all_new_items.extend(new_items)
        all_notes.extend(notes)

    new_ids = lakebase_broker.insert_itinerary_items(all_new_items)

    result = {"success": True, "items_added": len(new_ids), "notes": all_notes}
    lakebase_broker.log_agent_action(trip_id, "generate_itinerary", {"trip_id": trip_id}, result)
    return result


@mcp.tool()
def reschedule_activity(itinerary_item_id: str, reason: str, new_date: str = None) -> dict:
    """Move an already-scheduled activity to a different day and record
    why. USE FOR: responding to a bad forecast on the activity's current
    date — typically after calling get_live_conditions and finding
    should_reschedule_outdoor_activities is true. itinerary_item_id must be
    a real id from the trip's existing itinerary (e.g. from
    generate_itinerary's output) — never invent one. If new_date isn't
    given, picks the destination's best remaining day by cached
    precipitation. reason should be a short, human-readable explanation —
    it's stored and shown directly to the user, so write it for them, not
    as a log line.
    """
    item = lakebase_broker.get_itinerary_item(itinerary_item_id)
    if not item:
        return {"success": False, "reason": f"No itinerary item found with id {itinerary_item_id}."}

    overview = lakebase_broker.get_trip_overview(item["trip_id"])
    destination = next(
        (d for d in overview["destinations"] if any(a["id"] == item["activity_id"] for a in d["activities"])),
        None,
    )

    if new_date:
        target_date = date.fromisoformat(new_date)
    elif destination:
        ranked_days = _rank_days_by_weather(destination)
        candidates = [d for d in ranked_days if d != item["scheduled_date"]]
        target_date = candidates[0] if candidates else item["scheduled_date"]
    else:
        target_date = item["scheduled_date"]

    lakebase_broker.update_itinerary_item(
        itinerary_item_id, scheduled_date=target_date, status="rescheduled", reschedule_reason=reason,
    )

    result = {
        "success": True, "itinerary_item_id": itinerary_item_id,
        "new_date": target_date.isoformat(), "reason": reason,
    }
    lakebase_broker.log_agent_action(
        item["trip_id"], "reschedule_activity",
        {"itinerary_item_id": itinerary_item_id, "reason": reason, "new_date": new_date}, result,
    )
    return result


_CATEGORY_ITEMS = {
    "hiking": ["Hiking boots"],
    "water_sports": ["Swimwear", "Quick-dry towel"],
}


@mcp.tool()
def build_packing_list(trip_id: str) -> dict:
    """Suggest packing items based on this trip's activity categories and
    cached weather forecasts, and add any not already on the list. USE FOR:
    generating a starting packing list, or refreshing it after activities
    or weather change. Never removes or duplicates items already on the
    list.
    """
    overview = lakebase_broker.get_trip_overview(trip_id)
    if not overview:
        return {"success": False, "reason": f"No trip found with id {trip_id}."}

    suggested = {"Passport or ID"}
    for destination in overview["destinations"]:
        for activity in destination["activities"]:
            suggested.update(_CATEGORY_ITEMS.get(activity["category"], []))

        precip_values = [w["precipitation_probability_pct"] for w in destination["weather"]
                          if w["precipitation_probability_pct"] is not None]
        temp_low_values = [w["temperature_low_c"] for w in destination["weather"]
                            if w["temperature_low_c"] is not None]

        if precip_values and max(precip_values) >= weather_broker.UMBRELLA_THRESHOLD_PCT:
            suggested.add("Rain jacket")
        if temp_low_values and min(temp_low_values) <= weather_broker.COLD_THRESHOLD_C:
            suggested.add("Warm layers")

    added = lakebase_broker.insert_packing_items(trip_id, sorted(suggested))
    result = {"success": True, "items_added": added}
    lakebase_broker.log_agent_action(trip_id, "build_packing_list", {"trip_id": trip_id}, result)
    return result


@mcp.tool()
def move_or_remove_itinerary_item(itinerary_item_id: str, action: str, new_date: str = None) -> dict:
    """Move an itinerary item to a specific new date, or remove it from the
    itinerary entirely. USE FOR: direct user requests like "move the hike
    to Tuesday" or "take the museum off the schedule" — for weather-driven
    changes, prefer reschedule_activity instead, since it records a reason
    and can pick the new date itself. itinerary_item_id must be a real id
    from the trip's existing itinerary — never invent one. action must be
    'move' or 'remove'; 'move' requires new_date (ISO YYYY-MM-DD).
    """
    item = lakebase_broker.get_itinerary_item(itinerary_item_id)
    if not item:
        return {"success": False, "reason": f"No itinerary item found with id {itinerary_item_id}."}

    if action == "remove":
        lakebase_broker.soft_delete_itinerary_item(itinerary_item_id)
        result = {"success": True, "action": "removed", "itinerary_item_id": itinerary_item_id}
    elif action == "move":
        if not new_date:
            return {"success": False, "reason": "new_date is required for action='move'."}
        lakebase_broker.update_itinerary_item(
            itinerary_item_id, scheduled_date=date.fromisoformat(new_date), status="planned",
        )
        result = {"success": True, "action": "moved", "itinerary_item_id": itinerary_item_id, "new_date": new_date}
    else:
        return {"success": False, "reason": f"Unknown action '{action}' — must be 'move' or 'remove'."}

    lakebase_broker.log_agent_action(
        item["trip_id"], "move_or_remove_itinerary_item",
        {"itinerary_item_id": itinerary_item_id, "action": action, "new_date": new_date}, result,
    )
    return result


if __name__ == "__main__":
    mcp.run(transport="streamable-http")