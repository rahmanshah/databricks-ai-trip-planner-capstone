"""
Trip Planner UI — Databricks App "trip-planner-ui".

Deliberately "dumb": create/view trips, destinations, activities, and
toggle packing items. No scheduling logic lives here — generating or
changing the itinerary happens through the agent (chat panel, added in a
later step), not by duplicating mcp_server's logic in a second place.

No real auth system — every action here acts as a single default user
(get_or_create_default_user), which is a reasonable simplification for a
personal-use capstone app, not a multi-tenant product.
"""

import os
import uuid as uuid_module

from flask import Flask, abort, flash, redirect, render_template, request, url_for

import lakebase

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-only-not-secret")

DEFAULT_USER_EMAIL = "trip-planner-ui@local"


def _is_valid_uuid(value):
    """Same guard as mcp_server/lakebase_broker.py — a malformed id (e.g.
    someone hand-editing a URL) should be a clean 404, not a raw Postgres
    'invalid input syntax for type uuid' error."""
    try:
        uuid_module.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def get_or_create_default_user():
    rows = lakebase.query("SELECT id FROM users WHERE email = %s", (DEFAULT_USER_EMAIL,))
    if rows:
        return rows[0]["id"]
    return lakebase.execute_returning_id(
        "INSERT INTO users (display_name, email) VALUES (%s, %s) RETURNING id",
        ("Trip Planner User", DEFAULT_USER_EMAIL),
    )


# ---------------------------------------------------------------------------
# Trips
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    trips = lakebase.query(
        "SELECT id, title, start_date, end_date, status FROM trips "
        "WHERE is_deleted = false ORDER BY start_date NULLS LAST"
    )
    return render_template("index.html", trips=trips)


@app.route("/trips", methods=["POST"])
def create_trip():
    title = request.form.get("title", "").strip()
    start_date = request.form.get("start_date") or None
    end_date = request.form.get("end_date") or None
    if not title:
        flash("Trip title is required.", "error")
        return redirect(url_for("index"))

    owner_id = get_or_create_default_user()
    trip_id = lakebase.execute_returning_id(
        "INSERT INTO trips (owner_id, title, start_date, end_date) VALUES (%s, %s, %s, %s) RETURNING id",
        (owner_id, title, start_date, end_date),
    )
    flash(f'Trip "{title}" created.', "success")
    return redirect(url_for("trip_detail", trip_id=trip_id))


@app.route("/trips/<trip_id>")
def trip_detail(trip_id):
    if not _is_valid_uuid(trip_id):
        abort(404)

    trip_rows = lakebase.query(
        "SELECT id, title, start_date, end_date, status FROM trips WHERE id = %s AND is_deleted = false",
        (trip_id,),
    )
    if not trip_rows:
        abort(404)
    trip = trip_rows[0]

    destinations = lakebase.query(
        """
        SELECT id, name, latitude, longitude, description, arrival_date, departure_date
        FROM destinations WHERE trip_id = %s AND is_deleted = false
        ORDER BY arrival_date NULLS LAST
        """,
        (trip_id,),
    )
    dest_ids = [d["id"] for d in destinations]

    activities_by_dest = {d["id"]: [] for d in destinations}
    if dest_ids:
        placeholders = ",".join(["%s"] * len(dest_ids))
        activities = lakebase.query(
            f"""
            SELECT id, destination_id, name, category, is_outdoor, duration_minutes
            FROM activities WHERE destination_id IN ({placeholders}) AND is_deleted = false
            ORDER BY name
            """,
            tuple(dest_ids),
        )
        for a in activities:
            activities_by_dest.setdefault(a["destination_id"], []).append(a)
    for d in destinations:
        d["activities"] = activities_by_dest.get(d["id"], [])

    itinerary_items = lakebase.query(
        """
        SELECT ii.id, ii.scheduled_date, ii.scheduled_time, ii.status, ii.reschedule_reason,
               a.name AS activity_name, d.name AS destination_name
        FROM itinerary_items ii
        JOIN activities a ON a.id = ii.activity_id
        JOIN destinations d ON d.id = a.destination_id
        WHERE ii.trip_id = %s AND ii.is_deleted = false
        ORDER BY ii.scheduled_date NULLS LAST, ii.position
        """,
        (trip_id,),
    )

    packing_items = lakebase.query(
        """
        SELECT id, item_name, category, is_packed, added_by
        FROM packing_items WHERE trip_id = %s AND is_deleted = false
        ORDER BY is_packed, category, item_name
        """,
        (trip_id,),
    )

    return render_template(
        "trip_detail.html",
        trip=trip,
        destinations=destinations,
        itinerary_items=itinerary_items,
        packing_items=packing_items,
    )


# ---------------------------------------------------------------------------
# Destinations
# ---------------------------------------------------------------------------

@app.route("/trips/<trip_id>/destinations", methods=["POST"])
def add_destination(trip_id):
    if not _is_valid_uuid(trip_id):
        abort(404)

    name = request.form.get("name", "").strip()
    arrival_date = request.form.get("arrival_date") or None
    departure_date = request.form.get("departure_date") or None
    if not name:
        flash("Destination name is required.", "error")
        return redirect(url_for("trip_detail", trip_id=trip_id))

    # latitude/longitude/description are intentionally left NULL here —
    # pipeline/ingest_destinations.py fills them in on its next run.
    lakebase.execute(
        "INSERT INTO destinations (trip_id, name, arrival_date, departure_date) VALUES (%s, %s, %s, %s)",
        (trip_id, name, arrival_date, departure_date),
    )
    flash(f'Destination "{name}" added — run the ingestion pipeline to fetch its description and coordinates.', "success")
    return redirect(url_for("trip_detail", trip_id=trip_id))


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@app.route("/destinations/<destination_id>/activities", methods=["POST"])
def add_activity(destination_id):
    if not _is_valid_uuid(destination_id):
        abort(404)

    trip_id = request.form.get("trip_id")  # hidden field, so we can redirect back correctly
    name = request.form.get("name", "").strip()
    category = request.form.get("category", "other")
    is_outdoor = request.form.get("is_outdoor") == "on"
    duration_minutes = request.form.get("duration_minutes") or None

    if not name:
        flash("Activity name is required.", "error")
        return redirect(url_for("trip_detail", trip_id=trip_id))

    lakebase.execute(
        """
        INSERT INTO activities (destination_id, name, category, is_outdoor, duration_minutes)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (destination_id, name, category, is_outdoor, duration_minutes),
    )
    flash(f'Activity "{name}" added.', "success")
    return redirect(url_for("trip_detail", trip_id=trip_id))


# ---------------------------------------------------------------------------
# Packing list
# ---------------------------------------------------------------------------

@app.route("/packing/<item_id>/toggle", methods=["POST"])
def toggle_packing_item(item_id):
    if not _is_valid_uuid(item_id):
        abort(404)

    trip_id = request.form.get("trip_id")
    lakebase.execute(
        "UPDATE packing_items SET is_packed = NOT is_packed WHERE id = %s",
        (item_id,),
    )
    return redirect(url_for("trip_detail", trip_id=trip_id))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", 8000)))
