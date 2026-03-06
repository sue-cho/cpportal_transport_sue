"""
Data layer for the congestion dashboard. Reads from Supabase (Postgres).
"""
from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd
import requests

from db import get_connection


def _add_months(d: date, months: int) -> date:
    """Return date `months` months after d (same day if possible)."""
    year, month = d.year, d.month
    month += months
    while month > 12:
        month -= 12
        year += 1
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, min(d.day, 28))


def _to_date_param(s: str) -> str:
    """Convert YYYY-MM or YYYY-MM-DD to YYYY-MM-DD for Postgres date column."""
    s = (s or "").strip()
    if len(s) == 7 and s[4] == "-":
        return f"{s}-01"
    return s


def get_london_series(
    metric_type: str = "confirmed_vehicles",
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> pd.DataFrame:
    """Return monthly London CCZ series. Columns: month, vehicles."""
    conn = get_connection()
    try:
        q = """
            SELECT month, vehicles
            FROM public.london_ccz_monthly
            WHERE metric_type = %s
        """
        params = [metric_type]
        if start_month:
            q += " AND month >= %s::date"
            params.append(_to_date_param(start_month))
        if end_month:
            q += " AND month <= %s::date"
            params.append(_to_date_param(end_month))
        q += " ORDER BY month"
        return pd.read_sql(q, conn, params=params)
    finally:
        conn.close()


def get_singapore_series(
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> pd.DataFrame:
    """Return annual Singapore city traffic series. Columns: year, avg_daily_vehicles."""
    conn = get_connection()
    try:
        q = """
            SELECT year, avg_daily_vehicles
            FROM public.singapore_city_annual
            WHERE 1=1
        """
        params = []
        if start_year is not None:
            q += " AND year >= %s"
            params.append(start_year)
        if end_year is not None:
            q += " AND year <= %s"
            params.append(end_year)
        q += " ORDER BY year"
        df = pd.read_sql(q, conn, params=params)
    finally:
        conn.close()

    # Fallback: if DB table is empty, fetch directly from data.gov.sg so charts populate.
    if df is not None and not df.empty:
        return df
    try:
        resource_id = "d_3136f317a1f282a33fe7a2f6a907c047"
        base_url = "https://data.gov.sg/api/action/datastore_search"
        records: list[dict] = []
        offset = 0
        limit = 100
        while True:
            r = requests.get(
                base_url,
                params={"resource_id": resource_id, "limit": limit, "offset": offset},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                break
            result = data.get("result", {})
            batch = result.get("records", [])
            records.extend(batch)
            total = result.get("total", 0)
            if offset + len(batch) >= total or not batch:
                break
            offset += len(batch)
        rows = []
        for rec in records:
            try:
                year = int(rec.get("year", 0))
                vol = rec.get("ave_daily_traffic_volume_entering_city")
                if vol is None:
                    continue
                vol = float(str(vol).replace(",", ""))
                rows.append((year, vol))
            except (TypeError, ValueError):
                continue
        df2 = pd.DataFrame(rows, columns=["year", "avg_daily_vehicles"]).dropna()
        if start_year is not None:
            df2 = df2[df2["year"] >= int(start_year)]
        if end_year is not None:
            df2 = df2[df2["year"] <= int(end_year)]
        return df2.sort_values("year").reset_index(drop=True)
    except Exception:
        return df if df is not None else pd.DataFrame(columns=["year", "avg_daily_vehicles"])


def _month_str_to_first_last(s: str) -> Tuple[str, str]:
    """Convert 'YYYY-MM' to (first_day, last_day) as 'YYYY-MM-DD'."""
    s = (s or "").strip()
    if len(s) == 7 and s[4] == "-":
        y, m = int(s[:4]), int(s[5:7])
        first = date(y, m, 1).isoformat()
        if m == 12:
            last = date(y, 12, 31).isoformat()
        else:
            last = (date(y, m + 1, 1) - timedelta(days=1)).isoformat()
        return first, last
    return s, s


def get_london_baseline_vs_current(
    metric_type: str,
    baseline_start: str,
    baseline_end: str,
    current_start: str,
    current_end: str,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (baseline_avg, current_avg, pct_change). Averages are per-month."""
    b_first, b_last = _month_str_to_first_last(baseline_start)[0], _month_str_to_first_last(baseline_end)[1]
    c_first, c_last = _month_str_to_first_last(current_start)[0], _month_str_to_first_last(current_end)[1]
    b_start, b_end = b_first, b_last
    c_start, c_end = c_first, c_last
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN month >= %s::date AND month <= %s::date THEN vehicles END) AS baseline_avg,
                AVG(CASE WHEN month >= %s::date AND month <= %s::date THEN vehicles END) AS current_avg
            FROM public.london_ccz_monthly
            WHERE metric_type = %s
            """,
            (b_start, b_end, c_start, c_end, metric_type),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None
        base, curr = row[0], row[1]
        if base is None or curr is None or base == 0:
            return (float(base) if base is not None else None, float(curr) if curr is not None else None, None)
        base, curr = float(base), float(curr)
        pct = (curr - base) / base * 100.0
        return base, curr, float(pct)
    finally:
        conn.close()


def get_current_charging_before_after_london(
    metric_type: str = "confirmed_vehicles",
) -> Tuple[Optional[float], Optional[float], Optional[float], str, str]:
    """Current charging hours (Jun 2020) trend: post-COVID baseline (Jun 2022–May 2023) vs last 12 months. Avoids COVID skew. Returns (baseline_avg, last_year_avg, pct_change, baseline_label, last_year_label)."""
    # Post-COVID baseline: first full year after restrictions lifted (avoids 2020–2021 skew)
    baseline_start = date(2022, 6, 1)
    baseline_end = date(2023, 5, 31)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(month) FROM public.london_ccz_monthly WHERE metric_type = %s",
            (metric_type,),
        )
        row_max = cur.fetchone()
        max_month = row_max[0] if row_max and row_max[0] else None
        if not max_month:
            return None, None, None, "Jun 2020–May 2021", "—"
        if hasattr(max_month, "date"):
            last_end = max_month.date()
        elif isinstance(max_month, date):
            last_end = max_month
        else:
            last_end = max_month
        last_start = _add_months(last_end, -11)
        b_start = baseline_start.strftime("%Y-%m-%d")
        b_end = baseline_end.strftime("%Y-%m-%d")
        a_start = last_start.strftime("%Y-%m-%d")
        a_end = last_end.strftime("%Y-%m-%d")
        last_label = f"{last_start:%b %Y}–{last_end:%b %Y}"
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN month >= %s::date AND month <= %s::date THEN vehicles END) AS first_year_avg,
                AVG(CASE WHEN month >= %s::date AND month <= %s::date THEN vehicles END) AS last_year_avg
            FROM public.london_ccz_monthly
            WHERE metric_type = %s
            """,
            (b_start, b_end, a_start, a_end, metric_type),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None, "Jun 2022–May 2023", last_label
        baseline_avg, last_avg = row[0], row[1]
        if baseline_avg is None or last_avg is None or baseline_avg == 0:
            return (
                float(baseline_avg) if baseline_avg is not None else None,
                float(last_avg) if last_avg is not None else None,
                None,
                "Jun 2022–May 2023",
                last_label,
            )
        baseline_avg, last_avg = float(baseline_avg), float(last_avg)
        pct = (last_avg - baseline_avg) / baseline_avg * 100.0
        return baseline_avg, last_avg, float(pct), "Jun 2022–May 2023", last_label
    finally:
        conn.close()


def get_before_after_for_policy_london(
    policy_date: date,
    metric_type: str = "confirmed_vehicles",
) -> Tuple[Optional[float], Optional[float], Optional[float], str, str]:
    """3 years before vs 3 years after policy month. Returns (before_avg, after_avg, pct_change, before_label, after_label)."""
    policy_month = date(policy_date.year, policy_date.month, 1)
    before_end = _add_months(policy_month, -1)
    before_start = _add_months(policy_month, -36)
    after_start = policy_month
    after_end = _add_months(policy_month, 35)
    b_start = before_start.strftime("%Y-%m-%d")
    b_end = before_end.strftime("%Y-%m-%d")
    a_start = after_start.strftime("%Y-%m-%d")
    a_end = after_end.strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN month >= %s::date AND month <= %s::date THEN vehicles END) AS before_avg,
                AVG(CASE WHEN month >= %s::date AND month <= %s::date THEN vehicles END) AS after_avg
            FROM public.london_ccz_monthly
            WHERE metric_type = %s
            """,
            (b_start, b_end, a_start, a_end, metric_type),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None, f"{before_start:%b %Y}–{before_end:%b %Y}", f"{after_start:%b %Y}–{after_end:%b %Y}"
        before_avg, after_avg = row[0], row[1]
        if before_avg is None or after_avg is None or before_avg == 0:
            return (
                float(before_avg) if before_avg is not None else None,
                float(after_avg) if after_avg is not None else None,
                None,
                f"{before_start:%b %Y}–{before_end:%b %Y}",
                f"{after_start:%b %Y}–{after_end:%b %Y}",
            )
        before_avg, after_avg = float(before_avg), float(after_avg)
        pct = (after_avg - before_avg) / before_avg * 100.0
        return before_avg, after_avg, float(pct), f"{before_start:%b %Y}–{before_end:%b %Y}", f"{after_start:%b %Y}–{after_end:%b %Y}"
    finally:
        conn.close()


def get_before_after_for_policy_singapore(
    policy_year: int,
) -> Tuple[Optional[float], Optional[float], Optional[float], str, str]:
    """3 years before vs 3 years after policy year. Returns (before_avg, after_avg, pct_change, before_label, after_label)."""
    before_start_y = policy_year - 3
    before_end_y = policy_year - 1
    after_start_y = policy_year
    after_end_y = policy_year + 2  # 3 years after = policy_year, +1, +2
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN year >= %s AND year <= %s THEN avg_daily_vehicles END) AS before_avg,
                AVG(CASE WHEN year >= %s AND year <= %s THEN avg_daily_vehicles END) AS after_avg
            FROM public.singapore_city_annual
            """,
            (before_start_y, before_end_y, after_start_y, after_end_y),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None, f"{before_start_y}–{before_end_y}", f"{after_start_y}–{after_end_y}"
        before_avg, after_avg = row[0], row[1]
        if before_avg is None or after_avg is None or before_avg == 0:
            return (
                float(before_avg) if before_avg is not None else None,
                float(after_avg) if after_avg is not None else None,
                None,
                f"{before_start_y}–{before_end_y}",
                f"{after_start_y}–{after_end_y}",
            )
        before_avg, after_avg = float(before_avg), float(after_avg)
        pct = (after_avg - before_avg) / before_avg * 100.0
        return before_avg, after_avg, float(pct), f"{before_start_y}–{before_end_y}", f"{after_start_y}–{after_end_y}"
    finally:
        conn.close()


def get_singapore_baseline_vs_current(
    baseline_start_year: int,
    baseline_end_year: int,
    current_start_year: int,
    current_end_year: int,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (baseline_avg, current_avg, pct_change) for avg_daily_vehicles."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN year >= %s AND year <= %s THEN avg_daily_vehicles END) AS baseline_avg,
                AVG(CASE WHEN year >= %s AND year <= %s THEN avg_daily_vehicles END) AS current_avg
            FROM public.singapore_city_annual
            """,
            (baseline_start_year, baseline_end_year, current_start_year, current_end_year),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None
        base, curr = row[0], row[1]
        if base is None or curr is None or base == 0:
            return (float(base) if base is not None else None, float(curr) if curr is not None else None, None)
        base, curr = float(base), float(curr)
        pct = (curr - base) / base * 100.0
        return base, curr, float(pct)
    finally:
        conn.close()


def get_erp2_first_vs_latest_singapore(
    policy_year: int = 2024,
) -> Tuple[Optional[float], Optional[float], Optional[float], str, str]:
    """ERP 2.0 impact: first full year after policy vs most recent year.

    - policy_year: calendar year ERP 2.0 began (May 2024).
    - first_year: first full year after implementation (policy_year + 1, typically 2025). If data
      does not yet cover that year, falls back to policy_year.
    - latest_year: most recent year in singapore_city_annual.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(year) FROM public.singapore_city_annual")
        row_max = cur.fetchone()
        max_year = row_max[0] if row_max and row_max[0] is not None else None
        if max_year is None:
            return None, None, None, str(policy_year + 1), "—"
        first_year = policy_year + 1
        if max_year < first_year:
            first_year = policy_year
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN year = %s THEN avg_daily_vehicles END) AS first_year_avg,
                AVG(CASE WHEN year = %s THEN avg_daily_vehicles END) AS latest_year_avg
            FROM public.singapore_city_annual
            """,
            (first_year, max_year),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None, str(first_year), str(max_year)
        first_avg, latest_avg = row[0], row[1]
        if first_avg is None or latest_avg is None or first_avg == 0:
            return (
                float(first_avg) if first_avg is not None else None,
                float(latest_avg) if latest_avg is not None else None,
                None,
                str(first_year),
                str(max_year),
            )
        first_avg, latest_avg = float(first_avg), float(latest_avg)
        pct = (latest_avg - first_avg) / first_avg * 100.0
        return first_avg, latest_avg, float(pct), str(first_year), str(max_year)
    finally:
        conn.close()


def get_erp2_recent_comparison_singapore() -> Tuple[Optional[float], Optional[float], Optional[float], str, str]:
    """ERP 2.0 comparison: 2023–2024 vs 2024–present (annual series).

    Returns (baseline_avg, current_avg, pct_change, baseline_label, current_label).
    Note: Because this is annual data, "May 2024" is represented as year 2024 onward.
    """
    baseline_start_y, baseline_end_y = 2023, 2024
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(year) FROM public.singapore_city_annual")
        row_max = cur.fetchone()
        max_year = row_max[0] if row_max and row_max[0] is not None else None
        if max_year is None:
            return None, None, None, "2023–2024", "2024–present"
        after_start_y, after_end_y = 2024, int(max_year)
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN year >= %s AND year <= %s THEN avg_daily_vehicles END) AS baseline_avg,
                AVG(CASE WHEN year >= %s AND year <= %s THEN avg_daily_vehicles END) AS current_avg
            FROM public.singapore_city_annual
            """,
            (baseline_start_y, baseline_end_y, after_start_y, after_end_y),
        )
        row = cur.fetchone()
        cur.close()
        if not row or (row[0] is None and row[1] is None):
            return None, None, None, "2023–2024", "2024–present"
        base, curr = row[0], row[1]
        if base is None or curr is None or base == 0:
            return (
                float(base) if base is not None else None,
                float(curr) if curr is not None else None,
                None,
                "2023–2024",
                "2024–present",
            )
        base, curr = float(base), float(curr)
        pct = (curr - base) / base * 100.0
        return base, curr, float(pct), "2023–2024", "2024–present"
    finally:
        conn.close()


def get_latest_tfl_snapshot(limit: int = 100) -> pd.DataFrame:
    """Return latest TfL road status rows (by snapshot_time)."""
    conn = get_connection()
    try:
        q = """
            SELECT snapshot_time, corridor_id, corridor_name, status_severity, status_description
            FROM public.tfl_road_status_snapshots
            WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM public.tfl_road_status_snapshots)
            ORDER BY corridor_name
            LIMIT %s
        """
        return pd.read_sql(q, conn, params=[limit])
    finally:
        conn.close()


def get_latest_lta_snapshot(limit: int = 200) -> pd.DataFrame:
    """Return latest LTA speed band rows (by snapshot_time)."""
    conn = get_connection()
    try:
        q = """
            SELECT snapshot_time, road_segment_id, road_name, speed_band, road_category
            FROM public.lta_speed_band_snapshots
            WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM public.lta_speed_band_snapshots)
            ORDER BY road_name
            LIMIT %s
        """
        return pd.read_sql(q, conn, params=[limit])
    finally:
        conn.close()


def get_tfl_snapshot_summary_series(limit_snapshots: int = 24) -> pd.DataFrame:
    """Return recent TfL snapshots with corridor counts by status. Columns: snapshot_time, status_severity, count."""
    conn = get_connection()
    try:
        q = """
            WITH recent AS (
                SELECT DISTINCT snapshot_time
                FROM public.tfl_road_status_snapshots
                ORDER BY snapshot_time DESC
                LIMIT %s
            )
            SELECT s.snapshot_time, COALESCE(s.status_severity, 'Unknown') AS status_severity, COUNT(*) AS n
            FROM public.tfl_road_status_snapshots s
            INNER JOIN recent r ON r.snapshot_time = s.snapshot_time
            GROUP BY s.snapshot_time, COALESCE(s.status_severity, 'Unknown')
            ORDER BY s.snapshot_time ASC
        """
        return pd.read_sql(q, conn, params=[limit_snapshots])
    finally:
        conn.close()
