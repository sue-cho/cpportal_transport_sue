"""
db_client.py — Supabase query layer for congestion pricing dashboard.
Reads from city_vehicle_count view and related tables.

Backend selection:
  • REST: SUPABASE_URL + SUPABASE_KEY (supabase-py), e.g. Posit Connect / anon key.
  • Postgres: PGHOST, PGUSER, PGPASSWORD, PGDATABASE (+ optional PGPORT, PGSSLMODE)
    when REST credentials are not set — uses psycopg2 directly.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd


def _supabase_project_ref() -> str:
    """Resolve project ref from env, or from db.<ref>.supabase.co / https://<ref>.supabase.co."""
    r = (os.environ.get("SUPABASE_PROJECT_REF") or "").strip()
    if r:
        return r
    host = (os.environ.get("PGHOST") or "").strip().lower()
    m = re.match(r"^db\.([a-z0-9]+)\.supabase\.co$", host)
    if m:
        return m.group(1)
    url = (os.environ.get("SUPABASE_URL") or "").strip().lower()
    m = re.match(r"^https?://([a-z0-9]+)\.supabase\.co/?", url)
    if m:
        return m.group(1)
    return ""


try:
    from dotenv import load_dotenv

    _here = Path(__file__).resolve().parent
    _env_ref = _here / ".env"
    _env_root = _here.parent / ".env"
    # reference/.env first; repo root .env last with override=True so file values win over
    # empty exported vars (otherwise SUPABASE_PROJECT_REF in .env is ignored).
    if _env_ref.exists():
        load_dotenv(_env_ref, override=False)
    if _env_root.exists():
        load_dotenv(_env_root, override=True)

    _ref = _supabase_project_ref()
    if _ref and not os.environ.get("SUPABASE_URL", "").strip():
        os.environ["SUPABASE_URL"] = f"https://{_ref}.supabase.co"
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------

_client: Any = None


def _use_rest() -> bool:
    return bool(os.environ.get("SUPABASE_URL", "").strip() and os.environ.get("SUPABASE_KEY", "").strip())


def _pg_ready() -> bool:
    return all(os.environ.get(k, "").strip() for k in ("PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE"))


def _backend() -> str:
    if _use_rest():
        return "rest"
    if _pg_ready():
        return "pg"
    raise RuntimeError(
        "Configure either (SUPABASE_URL + SUPABASE_KEY) for the Supabase REST client, "
        "or PGHOST, PGUSER, PGPASSWORD, PGDATABASE for direct Postgres access."
    )


def get_client() -> Any:
    """Lazy singleton for supabase-py (REST mode only)."""
    global _client
    if _client is None:
        if not _use_rest():
            raise RuntimeError("get_client() is only valid when SUPABASE_URL and SUPABASE_KEY are set.")
        try:
            from supabase import create_client
        except ImportError as e:
            raise ImportError("supabase-py is required for REST mode: pip install supabase") from e
        url = os.environ.get("SUPABASE_URL", "").strip()
        key = os.environ.get("SUPABASE_KEY", "").strip()
        _client = create_client(url, key)
    return _client


def _resolve_pg_user() -> str:
    """
    Supabase Supavisor pooler (*.pooler.supabase.com) expects the database user
    'postgres.<project_ref>', not plain 'postgres'. Direct connections to
    db.<ref>.supabase.co use user 'postgres'.
    """
    user = (os.environ.get("PGUSER") or "").strip()
    host = (os.environ.get("PGHOST") or "").lower()
    if user.startswith("postgres.") and len(user) > len("postgres."):
        return user
    ref = _supabase_project_ref()
    if ref and "pooler.supabase.com" in host and user == "postgres":
        return f"postgres.{ref}"
    return user


def _pg_connect():
    import psycopg2

    port = int(os.environ.get("PGPORT") or "5432")
    sslmode = os.environ.get("PGSSLMODE", "require")
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=port,
        user=_resolve_pg_user(),
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
        sslmode=sslmode,
    )


def _coerce_observation_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ("obs_year", "obs_quarter", "obs_month", "period_ordinal"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "obs_date" in df.columns:
        df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
    for col in ("policy_start", "policy_end"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "treated" in df.columns:
        df["treated"] = df["treated"].astype(bool)
    return df


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

def _fetch_cities_pg() -> pd.DataFrame:
    from psycopg2.extras import RealDictCursor

    sql = "SELECT id, city_code, city_name, country_code, lat, lon, timezone FROM cities"
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def fetch_cities() -> pd.DataFrame:
    """Return all cities as a DataFrame (id, city_code, city_name, country_code, lat, lon)."""
    if _backend() == "pg":
        return _fetch_cities_pg()

    client = get_client()
    resp = client.table("cities").select("id, city_code, city_name, country_code, lat, lon, timezone").execute()
    if not resp.data:
        return pd.DataFrame()
    return pd.DataFrame(resp.data)


def _fetch_metric_units_pg() -> pd.DataFrame:
    from psycopg2.extras import RealDictCursor

    sql = "SELECT id, metric_type, unit, display_name, domain FROM metric_units"
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def fetch_metric_units() -> pd.DataFrame:
    """Return all metric_units rows for populating selectors."""
    if _backend() == "pg":
        return _fetch_metric_units_pg()

    client = get_client()
    resp = client.table("metric_units").select("id, metric_type, unit, display_name, domain").execute()
    if not resp.data:
        return pd.DataFrame()
    return pd.DataFrame(resp.data)


def _fetch_metric_types_for_city_pg(city_code: str) -> list[str]:
    from psycopg2.extras import RealDictCursor

    sql = """
        SELECT DISTINCT metric_type
        FROM city_vehicle_count
        WHERE city_code = %s AND metric_type IS NOT NULL
        ORDER BY metric_type
    """
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (city_code,))
            rows = cur.fetchall()
    if not rows:
        return []
    return [str(r["metric_type"]) for r in rows if r.get("metric_type") is not None]


def fetch_metric_types_for_city(city_code: str) -> list[str]:
    """
    Distinct metric_type values present in city_vehicle_count for this city.
    Used to limit the metric dropdown to metrics that exist for the selected city.
    """
    if not (city_code or "").strip():
        return []
    city_code = city_code.strip()
    if _backend() == "pg":
        return _fetch_metric_types_for_city_pg(city_code)

    client = get_client()
    try:
        resp = client.rpc("metric_types_for_city", {"p_city_code": city_code}).execute()
        if resp.data:
            out = [str(r["metric_type"]) for r in resp.data if r.get("metric_type") is not None]
            if out:
                return sorted(set(out))
    except Exception:
        pass

    try:
        resp = (
            client.table("city_vehicle_count")
            .select("metric_type")
            .eq("city_code", city_code)
            .limit(100_000)
            .execute()
        )
        if not resp.data:
            return []
        s = pd.Series([r.get("metric_type") for r in resp.data]).dropna().astype(str).unique()
        return sorted(set(s))
    except Exception:
        return []


def _fetch_policy_periods_pg(city_code: Optional[str] = None) -> pd.DataFrame:
    from psycopg2.extras import RealDictCursor

    cols = (
        "id, policy_uid, label, system_type, start_date, end_date, treated, "
        "period_ordinal, city_id, chart_mark_start"
    )
    if city_code:
        sql = f"""
            SELECT {cols}
            FROM congestion_pricing_periods
            WHERE city_id = (SELECT id FROM cities WHERE city_code = %s LIMIT 1)
            ORDER BY start_date
        """
        params: tuple[Any, ...] = (city_code,)
    else:
        sql = f"SELECT {cols} FROM congestion_pricing_periods ORDER BY start_date"
        params = ()

    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    for col in ("start_date", "end_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def fetch_policy_periods(city_code: Optional[str] = None) -> pd.DataFrame:
    """Return congestion_pricing_periods, optionally filtered by city_code."""
    if _backend() == "pg":
        return _fetch_policy_periods_pg(city_code)

    client = get_client()
    query = client.table("congestion_pricing_periods").select(
        "id, policy_uid, label, system_type, start_date, end_date, treated, "
        "period_ordinal, city_id, chart_mark_start"
    )
    if city_code:
        cities = fetch_cities()
        if not cities.empty:
            match = cities[cities["city_code"] == city_code]
            if not match.empty:
                city_id = match.iloc[0]["id"]
                query = query.eq("city_id", city_id)
    resp = query.order("start_date").execute()
    if not resp.data:
        return pd.DataFrame()
    df = pd.DataFrame(resp.data)
    for col in ("start_date", "end_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Main observation data
# ---------------------------------------------------------------------------

_OBS_COLS = (
    "id, city_code, city_name, country_code, "
    "metric_type, unit, metric_display_name, metric_domain, "
    "value, temporal_grain, obs_date, obs_year, obs_quarter, obs_month, "
    "source_api, capture_method, "
    "policy_uid, policy_label, policy_system_type, treated, "
    "policy_start, policy_end, period_ordinal"
)


def _fetch_observations_pg(
    city_codes: Optional[list[str]] = None,
    metric_type: Optional[str] = None,
    temporal_grain: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    from psycopg2.extras import RealDictCursor

    where: list[str] = ["TRUE"]
    params: list[Any] = []

    if city_codes:
        where.append("city_code = ANY(%s)")
        params.append(list(city_codes))
    if metric_type:
        where.append("metric_type = %s")
        params.append(metric_type)
    if temporal_grain:
        where.append("temporal_grain = %s")
        params.append(temporal_grain)
    if year_min is not None:
        where.append("obs_year >= %s")
        params.append(year_min)
    if year_max is not None:
        where.append("obs_year <= %s")
        params.append(year_max)

    lim_sql = ""
    if limit is not None and limit > 0:
        lim_sql = " LIMIT %s"
        params.append(int(limit))

    sql = f"SELECT {_OBS_COLS} FROM city_vehicle_count WHERE {' AND '.join(where)} ORDER BY obs_year{lim_sql}"

    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return _coerce_observation_df(pd.DataFrame([dict(r) for r in rows]))


def fetch_observations(
    city_codes: Optional[list[str]] = None,
    metric_type: Optional[str] = None,
    temporal_grain: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Query city_vehicle_count view with optional filters.
    Returns a tidy DataFrame ready for plotting.
    If limit is set, cap rows returned (useful for large views or smoke tests).
    """
    if _backend() == "pg":
        return _fetch_observations_pg(
            city_codes, metric_type, temporal_grain, year_min, year_max, limit
        )

    client = get_client()
    query = client.table("city_vehicle_count").select(
        "id, city_code, city_name, country_code, "
        "metric_type, unit, metric_display_name, metric_domain, "
        "value, temporal_grain, obs_date, obs_year, obs_quarter, obs_month, "
        "source_api, capture_method, "
        "policy_uid, policy_label, policy_system_type, treated, "
        "policy_start, policy_end, period_ordinal"
    )

    if city_codes:
        query = query.in_("city_code", city_codes)
    if metric_type:
        query = query.eq("metric_type", metric_type)
    if temporal_grain:
        query = query.eq("temporal_grain", temporal_grain)
    if year_min is not None:
        query = query.gte("obs_year", year_min)
    if year_max is not None:
        query = query.lte("obs_year", year_max)
    query = query.order("obs_year")
    if limit is not None and limit > 0:
        query = query.limit(int(limit))

    resp = query.execute()
    if not resp.data:
        return pd.DataFrame()

    df = pd.DataFrame(resp.data)
    return _coerce_observation_df(df)


# ---------------------------------------------------------------------------
# Derived / aggregated helpers
# ---------------------------------------------------------------------------

def compute_before_after(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate mean value by city and treated flag.
    When metric_type is present, groups by (city_code, city_name, metric_type, treated)
    so mixed metrics (e.g. London camera_captures vs Singapore avg_daily_vehicles) stay separate.
    Returns columns: city_code, city_name, treated, mean_value, n_obs, pct_change,
    and metric_type, unit when applicable.
    """
    if df.empty or "treated" not in df.columns:
        return pd.DataFrame()

    if "metric_type" in df.columns:
        agg = (
            df.groupby(["city_code", "city_name", "metric_type", "treated"])
            .agg(
                mean_value=("value", "mean"),
                n_obs=("value", "count"),
                unit=("unit", "first"),
            )
            .reset_index()
        )
        group_keys = agg[["city_code", "metric_type"]].drop_duplicates()
    else:
        agg = (
            df.groupby(["city_code", "city_name", "treated"])
            .agg(mean_value=("value", "mean"), n_obs=("value", "count"))
            .reset_index()
        )
        group_keys = agg[["city_code"]].drop_duplicates()

    result_rows = []
    if "metric_type" in agg.columns:
        for _, gk in group_keys.iterrows():
            city = gk["city_code"]
            mt = gk["metric_type"]
            city_df = agg[(agg["city_code"] == city) & (agg["metric_type"] == mt)]
            pre = city_df[~city_df["treated"]]
            post = city_df[city_df["treated"]]
            pre_mean = pre["mean_value"].values[0] if not pre.empty else None
            post_mean = post["mean_value"].values[0] if not post.empty else None
            pct = (
                round((post_mean - pre_mean) / pre_mean * 100, 1)
                if pre_mean and post_mean and pre_mean != 0
                else None
            )
            for _, row in city_df.iterrows():
                result_rows.append({**row.to_dict(), "pct_change": pct})
    else:
        for city in agg["city_code"].unique():
            city_df = agg[agg["city_code"] == city]
            pre = city_df[~city_df["treated"]]
            post = city_df[city_df["treated"]]
            pre_mean = pre["mean_value"].values[0] if not pre.empty else None
            post_mean = post["mean_value"].values[0] if not post.empty else None
            pct = (
                round((post_mean - pre_mean) / pre_mean * 100, 1)
                if pre_mean and post_mean and pre_mean != 0
                else None
            )
            for _, row in city_df.iterrows():
                result_rows.append({**row.to_dict(), "pct_change": pct})

    return pd.DataFrame(result_rows) if result_rows else pd.DataFrame()
