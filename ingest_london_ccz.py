"""
Ingest London Congestion Charge Zone monthly data into Supabase.
Download CSV from London Datastore, then upsert into public.london_ccz_monthly.

Run manually: python ingest_london_ccz.py
"""
import io
from typing import Optional

import pandas as pd
import requests

from db import get_connection

# London Datastore CSV (Camera Captures and Confirmed Vehicles by month)
LONDON_CCZ_CSV_URL = (
    "https://data.london.gov.uk/download/vehicles-entering-c-charge-zone-month"
    "/601a15a2-352c-46be-adae-e049556314a3/tfl-vehicles-c-charge-zone.csv"
)


def _safe_int(val) -> Optional[int]:
    """Parse integer from string or number; strip commas and handle empty/NaN."""
    if pd.isna(val) or val == "" or val is None:
        return None
    s = str(val).strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def fetch_london_ccz_csv() -> pd.DataFrame:
    """Download and parse the London CCZ CSV. Returns DataFrame with columns: month, metric_type, vehicles."""
    r = requests.get(LONDON_CCZ_CSV_URL, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    # Normalize column names (allow various capitalizations)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    # Month column: "month" or "period" or first column
    month_col = next((c for c in df.columns if "month" in c or c == "period"), None)
    if month_col is None and len(df.columns) > 0:
        month_col = df.columns[0]
    if month_col is None:
        raise ValueError("CSV has no usable date column. Columns: " + str(list(df.columns)))
    # Skip header-like rows in the month column
    raw = df[month_col].astype(str).str.strip()
    df = df.loc[~raw.str.lower().eq("month")].copy()
    raw = df[month_col].astype(str).str.strip()
    # Parse month: CSV uses "Jul-10", "Aug-10" (Mon-YY) per London Datastore
    raw_month = pd.to_datetime(raw, format="%b-%y", errors="coerce")
    if raw_month.isna().all():
        raw_month = pd.to_datetime(raw, dayfirst=True, errors="coerce")
    if raw_month.isna().all():
        def with_day(s):
            if len(s) == 7 and s[4] == "-":  # 2010-07
                return s + "-01"
            if len(s) == 6 and s.isdigit():   # 201007
                return s[:4] + "-" + s[4:6] + "-01"
            return s
        raw_month = pd.to_datetime(raw.map(with_day), format="%Y-%m-%d", errors="coerce")
    df["month"] = raw_month.dt.normalize()
    df = df.dropna(subset=["month"])
    # Value columns: London Datastore uses cc_camera_captures_* and cc_confirmed_vehicles_*
    cap_col = next((c for c in df.columns if "camera_capture" in c or "capture" in c), None)
    conf_col = next((c for c in df.columns if "confirmed_vehicle" in c or "confirm" in c), None)
    rows = []
    for _, row in df.iterrows():
        m = row["month"]
        if cap_col is not None:
            v = _safe_int(row.get(cap_col))
            if v is not None:
                rows.append({"month": m, "metric_type": "camera_captures", "vehicles": v})
        if conf_col is not None and cap_col != conf_col:
            v = _safe_int(row.get(conf_col))
            if v is not None:
                rows.append({"month": m, "metric_type": "confirmed_vehicles", "vehicles": v})
    out = pd.DataFrame(rows)
    if out.empty:
        raise ValueError(
            "No rows parsed from CSV. Columns seen: %s. First row: %s"
            % (list(df.columns), df.head(1).to_dict() if not df.empty else "N/A")
        )
    return out


def upsert_london_ccz(conn, df: pd.DataFrame) -> int:
    """Insert or update rows in london_ccz_monthly. Returns number of rows affected."""
    cur = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        month = row["month"]
        if hasattr(month, "date"):
            month = month.date()
        metric_type = row["metric_type"]
        vehicles = int(row["vehicles"])
        cur.execute(
            """
            INSERT INTO public.london_ccz_monthly (month, metric_type, vehicles)
            VALUES (%s, %s, %s)
            ON CONFLICT (month, metric_type) DO UPDATE SET
              vehicles = EXCLUDED.vehicles,
              created_at = now()
            """,
            (month, metric_type, vehicles),
        )
        count += cur.rowcount
    conn.commit()
    cur.close()
    return count


def main():
    print("Fetching London CCZ data from London Datastore...")
    df = fetch_london_ccz_csv()
    print(f"Parsed {len(df)} rows (month × metric_type).")
    print("Connecting to Supabase...")
    conn = get_connection()
    try:
        n = upsert_london_ccz(conn, df)
        print(f"Upserted {n} rows into public.london_ccz_monthly.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
