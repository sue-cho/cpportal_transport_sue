import os
from typing import Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv


BASE_URL = "https://data.cityofnewyork.us/resource/7ym2-wayt.json"


def fetch_traffic_data(
    start_year: int = 2024,
    end_year: int = 2025,
    app_token: Optional[str] = None,
    limit: int = 500_000,
) -> pd.DataFrame:
    """
    Fetch NYC Automated Traffic Volume Counts data from NYC Open Data (Socrata).

    Data fields of interest include:
      - boro: Borough name (e.g., Queens, Manhattan)
      - yr, m, d: Year, month, day
      - hh, mm: Hour and minute of the count
      - vol: Vehicle count for that 15‑minute interval

    This function:
      - Filters rows server‑side by year using SoQL
      - Casts numeric fields
      - Builds a proper pandas datetime column named 'date'
    """

    # Load variables from .env once per process; safe to call multiple times.
    load_dotenv()

    if app_token is None:
        # Prefer explicit API key from .env, fall back to old name if present.
        app_token = (
            os.getenv("NYC_OPENDATA_API_KEY")
            or os.getenv("NYC_OPENDATA_APP_TOKEN")
        )

    where_clause = f"yr >= '{start_year}' AND yr <= '{end_year}'"

    params = {
        "$select": "requestid,boro,yr,m,d,hh,mm,vol,segmentid,street,fromst,tost,direction",
        "$where": where_clause,
        "$limit": limit,
    }

    headers: dict = {}
    if app_token:
        headers["X-App-Token"] = app_token
    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        records = response.json()
    except requests.RequestException as exc:
        # On any request/API error, return an empty frame so the app
        # can display a friendly "no data" message instead of crashing.
        print(f"Error fetching NYC traffic data: {exc}")
        return pd.DataFrame(
            columns=[
                "requestid",
                "boro",
                "yr",
                "m",
                "d",
                "hh",
                "mm",
                "vol",
                "segmentid",
                "street",
                "fromst",
                "tost",
                "direction",
                "date",
            ]
        )

    if not records:
        return pd.DataFrame(
            columns=[
                "requestid",
                "boro",
                "yr",
                "m",
                "d",
                "hh",
                "mm",
                "vol",
                "segmentid",
                "street",
                "fromst",
                "tost",
                "direction",
                "date",
            ]
        )

    df = pd.DataFrame.from_records(records)

    # Coerce numeric fields
    for col in ("vol", "yr", "m", "d", "hh", "mm"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build a proper date column (no time component for daily aggregations)
    if {"yr", "m", "d"}.issubset(df.columns):
        date_parts = df[["yr", "m", "d"]].rename(
            columns={"yr": "year", "m": "month", "d": "day"}
        )
        df["date"] = pd.to_datetime(date_parts, errors="coerce")
    else:
        df["date"] = pd.NaT

    return df


def build_aggregates(
    df: pd.DataFrame,
    pricing_start: str = "2025-01-01",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Given raw traffic data, compute:
      - daily aggregated vehicle counts per borough
      - monthly aggregated counts, split into pre/post congestion pricing periods

    Returns
    -------
    daily_df : DataFrame
        Columns: ['date', 'boro', 'daily_volume', 'period']
    monthly_df : DataFrame
        Columns: ['month', 'boro', 'period', 'monthly_volume']
    """

    if df.empty:
        daily_empty = pd.DataFrame(
            columns=["date", "boro", "daily_volume", "period"]
        )
        monthly_empty = pd.DataFrame(
            columns=["month", "boro", "period", "monthly_volume"]
        )
        return daily_empty, monthly_empty

    working = df.copy()

    # Ensure required columns exist
    for required in ("date", "boro", "vol"):
        if required not in working.columns:
            raise ValueError(f"Required column '{required}' missing from dataframe.")

    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working = working.dropna(subset=["date", "boro", "vol"])

    # Daily total volume per borough
    daily = (
        working.groupby(["date", "boro"], as_index=False)["vol"]
        .sum()
        .rename(columns={"vol": "daily_volume"})
    )

    pricing_start_ts = pd.to_datetime(pricing_start)
    daily["period"] = (
        daily["date"]
        .apply(lambda d: "Before 2025-01-01" if d < pricing_start_ts else "On/After 2025-01-01")
        .astype("category")
    )

    # Monthly aggregation (sum of daily volumes per month/boro/period)
    daily["month"] = daily["date"].dt.to_period("M").dt.to_timestamp()
    monthly = (
        daily.groupby(["month", "boro", "period"], as_index=False)["daily_volume"]
        .sum()
        .rename(columns={"daily_volume": "monthly_volume"})
    )

    return daily, monthly

