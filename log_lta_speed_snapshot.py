"""
Log a snapshot of LTA Traffic Speed Bands to Supabase.
Call periodically (e.g. manually or via cron) to build a time series.

Requires in .env: LTA_APP_KEY (or LTA_ACCOUNT_KEY / SINGAPORE_TOKEN)
Run: python log_lta_speed_snapshot.py
"""
import json
import os
from datetime import datetime, timezone
from typing import List

import requests

from db import get_connection

# LTA DataMall v4 Traffic Speed Bands (override with LTA_DATAMALL_BASE_URL in .env if needed)
LTA_SPEED_BANDS_URL = os.getenv(
    "LTA_SPEED_BANDS_URL",
    "https://datamall2.mytransport.sg/ltaodataservice/v4/TrafficSpeedBands",
)


def fetch_lta_speed_bands(account_key: str) -> List[dict]:
    """Fetch current Traffic Speed Bands. Handles OData pagination if present."""
    headers = {"AccountKey": account_key, "Accept": "application/json"}
    records = []
    url = LTA_SPEED_BANDS_URL.strip()
    while url:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        value = data.get("value", data) if isinstance(data, dict) else data
        if isinstance(value, list):
            records.extend(value)
        url = data.get("odata.nextLink") or data.get("@odata.nextLink") if isinstance(data, dict) else None
    return records


def log_snapshot(conn, records: List[dict]) -> int:
    """Insert one row per segment into lta_speed_band_snapshots. Returns row count."""
    snapshot_time = datetime.now(timezone.utc)
    cur = conn.cursor()
    count = 0
    for rec in records:
        segment_id = rec.get("LinkID") or rec.get("linkId") or rec.get("RoadSegmentID")
        road_name = rec.get("RoadName") or rec.get("roadName")
        speed_band = rec.get("SpeedBand") or rec.get("speedBand")
        if speed_band is not None and not isinstance(speed_band, int):
            try:
                speed_band = int(speed_band)
            except (TypeError, ValueError):
                speed_band = None
        road_category = rec.get("RoadCategory") or rec.get("roadCategory")
        cur.execute(
            """
            INSERT INTO public.lta_speed_band_snapshots
            (snapshot_time, road_segment_id, road_name, speed_band, road_category, json_raw)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_time,
                str(segment_id) if segment_id is not None else None,
                str(road_name) if road_name is not None else None,
                speed_band,
                str(road_category) if road_category is not None else None,
                json.dumps(rec) if rec else None,
            ),
        )
        count += cur.rowcount
    conn.commit()
    cur.close()
    return count


def main():
    account_key = (
        os.getenv("LTA_APP_KEY")
        or os.getenv("LTA_ACCOUNT_KEY")
        or os.getenv("SINGAPORE_TOKEN")
    )
    if not account_key:
        print("Set LTA_APP_KEY (or LTA_ACCOUNT_KEY / SINGAPORE_TOKEN) in .env (from LTA DataMall).")
        return
    print("Fetching LTA Traffic Speed Bands...")
    records = fetch_lta_speed_bands(account_key.strip())
    print(f"Got {len(records)} segments. Logging to Supabase...")
    conn = get_connection()
    try:
        n = log_snapshot(conn, records)
        print(f"Inserted {n} rows into lta_speed_band_snapshots.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
