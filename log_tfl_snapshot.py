"""
Log a snapshot of TfL road corridor status to Supabase.
Call periodically (e.g. manually or via cron) to build a time series of traffic conditions.

Requires in .env: TFL_APP_KEY
Run: python log_tfl_snapshot.py
"""
import json
import os
from datetime import datetime, timezone

import requests

from db import get_connection

TFL_ROAD_STATUS_URL = "https://api.tfl.gov.uk/Road/all/Status"


def fetch_tfl_road_status(app_key: str):
    """Fetch current status for all TfL road corridors. Returns list of corridor dicts."""
    r = requests.get(
        TFL_ROAD_STATUS_URL,
        params={"app_key": app_key},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def log_snapshot(conn, corridors: list) -> int:
    """Insert one row per corridor into tfl_road_status_snapshots. Returns row count."""
    snapshot_time = datetime.now(timezone.utc)
    cur = conn.cursor()
    count = 0
    for corr in corridors:
        corridor_id = corr.get("id") or corr.get("displayName") or ""
        if not corridor_id:
            continue
        name = corr.get("displayName") or corr.get("name")
        severity = corr.get("statusSeverity")
        desc = corr.get("statusSeverityDescription")
        if isinstance(severity, dict):
            severity = severity.get("description") if isinstance(severity, dict) else str(severity)
        if isinstance(desc, dict):
            desc = desc.get("description") if isinstance(desc, dict) else str(desc)
        cur.execute(
            """
            INSERT INTO public.tfl_road_status_snapshots
            (snapshot_time, corridor_id, corridor_name, status_severity, status_description, json_raw)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_time,
                str(corridor_id),
                name,
                str(severity) if severity is not None else None,
                str(desc) if desc is not None else None,
                json.dumps(corr) if corr else None,
            ),
        )
        count += cur.rowcount
    conn.commit()
    cur.close()
    return count


def main():
    app_key = os.getenv("TFL_APP_KEY")
    if not app_key:
        print("Set TFL_APP_KEY in .env (from https://api-portal.tfl.gov.uk/)")
        return
    print("Fetching TfL road status...")
    corridors = fetch_tfl_road_status(app_key)
    if not isinstance(corridors, list):
        print("Unexpected response format:", type(corridors))
        return
    print(f"Got {len(corridors)} corridors. Logging to Supabase...")
    conn = get_connection()
    try:
        n = log_snapshot(conn, corridors)
        print(f"Inserted {n} rows into tfl_road_status_snapshots.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
