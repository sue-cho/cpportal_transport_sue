"""
Ingest Singapore 'Average Daily Traffic Volume Entering the City' into Supabase.
Fetches from data.gov.sg API, then upserts into public.singapore_city_annual.

Run manually: python ingest_singapore_annual.py
"""
from typing import List, Tuple

import requests

from db import get_connection

DATA_GOV_SG_RESOURCE_ID = "d_3136f317a1f282a33fe7a2f6a907c047"
BASE_URL = "https://data.gov.sg/api/action/datastore_search"
TIME_WINDOW_DESC = "07:30–19:00 weekdays"


def fetch_singapore_city_annual() -> List[dict]:
    """Fetch all records from data.gov.sg. Returns list of {year, avg_daily_vehicles}."""
    records = []
    offset = 0
    limit = 100
    while True:
        r = requests.get(
            BASE_URL,
            params={"resource_id": DATA_GOV_SG_RESOURCE_ID, "limit": limit, "offset": offset},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError("API returned success=false: " + str(data))
        result = data.get("result", {})
        batch = result.get("records", [])
        records.extend(batch)
        total = result.get("total", 0)
        if offset + len(batch) >= total or not batch:
            break
        offset += len(batch)
    return records


def parse_records(records: List[dict]) -> List[Tuple[int, float]]:
    """Return list of (year, avg_daily_vehicles)."""
    out = []
    for rec in records:
        try:
            year = int(rec.get("year", 0))
            vol = rec.get("ave_daily_traffic_volume_entering_city")
            if vol is None:
                continue
            vol = float(str(vol).replace(",", ""))
            out.append((year, vol))
        except (TypeError, ValueError):
            continue
    return out


def upsert_singapore_annual(conn, rows: List[Tuple[int, float]]) -> int:
    """Insert or update public.singapore_city_annual. Returns number of rows affected."""
    cur = conn.cursor()
    count = 0
    for year, avg_daily in rows:
        cur.execute(
            """
            INSERT INTO public.singapore_city_annual (year, avg_daily_vehicles, time_window_desc)
            VALUES (%s, %s, %s)
            ON CONFLICT (year) DO UPDATE SET
              avg_daily_vehicles = EXCLUDED.avg_daily_vehicles,
              time_window_desc = EXCLUDED.time_window_desc,
              created_at = now()
            """,
            (year, avg_daily, TIME_WINDOW_DESC),
        )
        count += cur.rowcount
    conn.commit()
    cur.close()
    return count


def main():
    print("Fetching Singapore city traffic data from data.gov.sg...")
    records = fetch_singapore_city_annual()
    rows = parse_records(records)
    print(f"Parsed {len(rows)} years.")
    if not rows:
        print("No data to upsert.")
        return
    print("Connecting to Supabase...")
    conn = get_connection()
    try:
        n = upsert_singapore_annual(conn, rows)
        print(f"Upserted {n} rows into public.singapore_city_annual.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
