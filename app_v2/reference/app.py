"""
Congestion Pricing Research Dashboard — Shiny for Python.
Single-city analysis mode: charts and tables use the primary city only.
Optional reference cities load separate observations for AI context (not shown in charts).
Data from city_vehicle_count via db_client.py; map via map_utils.py; AI via llm_cloud.py.
"""

from __future__ import annotations

import base64
import datetime
import html
import re
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
from shiny import App, reactive, render, ui

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from db_client import (
    fetch_cities,
    fetch_metric_types_for_city,
    fetch_metric_units,
    fetch_observations,
    fetch_policy_periods,
)
from map_utils import build_city_map

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLICY_COLORS = {
    True:  "#0f6e56",   # teal  — treated / policy active
    False: "#888780",   # gray  — untreated / pre-policy
}

CITY_PALETTE = {
    "SGP": "#185FA5",
    "LON": "#993C1D",
}

# When one city plots several metrics (e.g. London), keep traces visually distinct
METRIC_LINE_COLORS = {
    "camera_captures": "#993C1D",
    "confirmed_vehicles": "#2563eb",
    "avg_daily_vehicles": "#185FA5",
    "vehicle_count": "#0d9488",
}

FAVICON_SVG = (
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E"
    "%3Ccircle cx='16' cy='16' r='14' fill='%231e293b'/%3E"
    "%3Cpath fill='white' d='M8 18l4-8 4 6 3-4 5 6H8z'/%3E%3C/svg%3E"
)

# ---------------------------------------------------------------------------
# Helpers (reused from Bar Harbor)
# ---------------------------------------------------------------------------

def sanitize_report_text(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return (raw or "").strip()
    s = raw
    s = re.sub(r"\*\*([^*]*)\*\*", r"\1", s)
    s = re.sub(r"__([^_]*)__", r"\1", s)
    s = re.sub(r"^#{1,6}\s*", "", s, flags=re.MULTILINE)
    s = s.replace("`", "")
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    s = s.replace("\u2018", "'").replace("\u2019", "'")
    s = s.replace("\u201c", '"').replace("\u201d", '"')
    s = re.sub(r"\n{3,}", "\n\n", s)
    lines = [line.rstrip() for line in s.splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines).strip()


def _monitoring_metrics(df: pd.DataFrame) -> dict[str, Any]:
    """
    Monitoring stats for one metric series (primary city), no pre/post policy requirement.
    Uses annual mean time series: mean value per obs_year.
    """
    out: dict[str, Any] = {
        "mixed_metrics": False,
        "primary_name": None,
        "primary_code": None,
        "n_obs": 0,
        "unit": "vehicles",
        "latest_value": None,
        "latest_year": None,
        "recent_avg": None,
        "recent_trend_pct": None,
        "volatility_label": None,
        "volatility_std": None,
    }
    if df.empty or "obs_year" not in df.columns or "value" not in df.columns:
        return out
    if "metric_type" in df.columns and df["metric_type"].nunique() > 1:
        out["mixed_metrics"] = True
        out["primary_name"] = df["city_name"].iloc[0] if "city_name" in df.columns else None
        out["primary_code"] = df["city_code"].iloc[0] if "city_code" in df.columns else None
        out["n_obs"] = len(df)
        out["unit"] = "mixed metrics"
        return out

    out["primary_name"] = df["city_name"].iloc[0] if "city_name" in df.columns else None
    out["primary_code"] = df["city_code"].iloc[0] if "city_code" in df.columns else None
    out["n_obs"] = len(df)
    out["unit"] = str(df["unit"].iloc[0]) if "unit" in df.columns and pd.notna(df["unit"].iloc[0]) else "vehicles"

    annual = df.groupby("obs_year", as_index=True)["value"].mean().sort_index()
    if annual.empty:
        return out

    annual.index = annual.index.astype(int)
    years = annual.index.values
    vals = annual.values.astype(float)
    latest_year = int(years[-1])
    out["latest_year"] = latest_year
    out["latest_value"] = float(vals[-1])

    last3 = vals[-3:] if len(vals) >= 3 else vals
    out["recent_avg"] = float(np.mean(last3)) if len(last3) else None

    target_year = latest_year - 3
    if target_year in annual.index:
        v3 = float(annual.loc[target_year])
        if v3 != 0 and not np.isnan(v3):
            out["recent_trend_pct"] = round((float(out["latest_value"]) - v3) / v3 * 100, 1)

    last5 = vals[-5:] if len(vals) >= 5 else vals
    if len(last5) >= 2:
        std = float(np.std(last5, ddof=1))
        mu = float(np.mean(last5))
        out["volatility_std"] = std
        cv = (std / mu) if mu and mu > 0 else float("inf")
        if cv < 0.03:
            out["volatility_label"] = "Low variability"
        elif cv < 0.10:
            out["volatility_label"] = "Medium variability"
        else:
            out["volatility_label"] = "High variability"
    return out


def _annual_means_for_chart(df: pd.DataFrame) -> pd.Series:
    """Mean value per calendar year (single metric)."""
    if df.empty or "obs_year" not in df.columns or "value" not in df.columns:
        return pd.Series(dtype=float)
    if "metric_type" in df.columns and df["metric_type"].nunique() > 1:
        return pd.Series(dtype=float)
    return df.groupby("obs_year")["value"].mean().sort_index()


def _trend_direction(df: pd.DataFrame) -> str:
    """Qualitative direction from the annual mean series (within-city only)."""
    if df.empty or "obs_year" not in df.columns or "value" not in df.columns:
        return "unknown"
    annual = df.groupby("obs_year", as_index=False)["value"].mean().sort_values("obs_year")
    if len(annual) < 2:
        return "insufficient timeline"
    y = annual["value"].astype(float).values
    years = annual["obs_year"].astype(float).values
    if np.nanstd(y) == 0:
        return "flat"
    slope = float(np.polyfit(years, y, 1)[0])
    y_spread = float(np.nanmax(y) - np.nanmin(y))
    if y_spread <= 0:
        return "flat"
    span = max(float(np.nanmax(years) - np.nanmin(years)), 1.0)
    scale = y_spread / span
    if scale <= 0:
        return "roughly stable"
    if slope > scale * 0.12:
        return "increasing"
    if slope < -scale * 0.12:
        return "decreasing"
    return "roughly stable"


def _policy_timing_treated(policy_df: pd.DataFrame, city_code: str) -> str:
    """Policy-on windows as calendar years for structured AI context."""
    if policy_df.empty or "city_code" not in policy_df.columns:
        return "no policy periods loaded"
    sub = policy_df[policy_df["city_code"] == city_code]
    if sub.empty:
        return "no periods for this city"
    treated = sub[sub["treated"] == True] if "treated" in sub.columns else sub
    if treated.empty:
        return "no treated periods listed"
    parts: list[str] = []
    for _, row in treated.iterrows():
        sy = row["start_date"].year if pd.notna(row.get("start_date")) else None
        ey = row["end_date"].year if pd.notna(row.get("end_date")) else None
        if sy is not None:
            parts.append(f"{sy}–{ey if ey else 'ongoing'}")
    return "; ".join(parts) if parts else "timing unavailable"


def _latest_policy_brief(policy_df: pd.DataFrame, city_code: str) -> str:
    """Most recent policy row by start_date (prefers treated) for AI focus."""
    if policy_df.empty or "city_code" not in policy_df.columns:
        return "no policy periods loaded"
    sub = policy_df[policy_df["city_code"] == city_code].copy()
    if sub.empty:
        return "no policy rows for this city"
    if "start_date" not in sub.columns:
        return "policy rows have no start_date"
    sub["start_date"] = pd.to_datetime(sub["start_date"], errors="coerce")
    sub = sub.dropna(subset=["start_date"])
    if sub.empty:
        return "no dated policy periods"
    if "treated" in sub.columns:
        tr = sub[sub["treated"] == True]
        pick = tr.sort_values("start_date", ascending=False) if not tr.empty else sub.sort_values(
            "start_date", ascending=False
        )
    else:
        pick = sub.sort_values("start_date", ascending=False)
    row = pick.iloc[0]
    uid = row.get("policy_uid", "—")
    lab = row.get("label", "—")
    sd = str(row["start_date"])[:10]
    ed = str(row["end_date"])[:10] if pd.notna(row.get("end_date")) else "ongoing"
    tr_s = "treated" if row.get("treated") is True else ("untreated" if row.get("treated") is False else "unknown")
    st = str(row.get("system_type") or "—")
    return f"policy_uid={uid}; label={lab}; system_type={st}; start={sd}; end={ed}; status={tr_s}"


def _line_color_for_metric(metric_type: Optional[str], city_code: str, fallback_idx: int) -> str:
    if metric_type and metric_type in METRIC_LINE_COLORS:
        return METRIC_LINE_COLORS[metric_type]
    base = CITY_PALETTE.get(city_code, "#64748b")
    return base if fallback_idx % 2 == 0 else "#475569"


def _aggregate_quarterly_mean(city_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse monthly (or duplicate) rows to one point per calendar quarter: mean(value)."""
    if city_df.empty:
        return city_df
    if "obs_year" not in city_df.columns or "obs_quarter" not in city_df.columns:
        return city_df
    if not city_df["obs_quarter"].notna().any():
        return city_df
    gcols = [c for c in ("city_code", "metric_type", "obs_year", "obs_quarter") if c in city_df.columns]
    agg_map: dict[str, str] = {"value": "mean"}
    for c in ("city_name", "unit"):
        if c in city_df.columns:
            agg_map[c] = "first"
    return city_df.groupby(gcols, as_index=False, dropna=False).agg(agg_map)


def _prepare_trend_plot_data(city_df: pd.DataFrame) -> tuple[pd.DataFrame, Any, str]:
    """
    Chronological x so quarterly observations do not stack on a single year tick.
    Returns sorted frame, x values, and x_kind: 'datetime', 'quarter_float', or 'year_float'.
    Quarter-first: when obs_quarter is present, map Q1–Q4 to y+0, y+0.25, y+0.5, y+0.75.
    """
    d = city_df.copy()
    if "obs_year" in d.columns and "obs_quarter" in d.columns and d["obs_quarter"].notna().any():
        y = d["obs_year"].astype(float)
        q = d["obs_quarter"].astype(float).fillna(1.0).clip(1.0, 4.0)
        d["_trend_x"] = y + (q - 1.0) / 4.0
        d = d.sort_values("_trend_x")
        return d, d["_trend_x"], "quarter_float"

    if "obs_date" in d.columns and d["obs_date"].notna().all():
        d["_trend_x"] = pd.to_datetime(d["obs_date"], errors="coerce")
        if d["_trend_x"].notna().all():
            d = d.sort_values("_trend_x")
            return d, d["_trend_x"], "datetime"

    if "obs_year" in d.columns:
        d["_trend_x"] = d["obs_year"].astype(float) + 0.5
        d = d.sort_values("_trend_x")
        return d, d["_trend_x"], "year_float"

    d = d.reset_index(drop=True)
    d["_trend_x"] = np.arange(len(d), dtype=float)
    return d, d["_trend_x"], "year_float"


def _policy_date_to_trend_x(ts: pd.Timestamp, x_kind: Optional[str]) -> Any:
    """Map a calendar start_date to the same x scale as the trend chart (quarter_float / year / datetime)."""
    if x_kind == "datetime":
        return pd.Timestamp(ts)
    if x_kind == "quarter_float":
        y = int(ts.year)
        q = (int(ts.month) - 1) // 3 + 1
        return float(y) + (q - 1) / 4.0
    return float(ts.year) + 0.5


def _plotly_to_iframe_html(fig: go.Figure, height: int, div_id: str) -> str:
    try:
        raw = fig.to_html(
            include_plotlyjs="cdn",
            config={"displayModeBar": True},
            div_id=div_id,
        )
        b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        return f'<iframe src="data:text/html;base64,{b64}" width="100%" height="{height}" style="border:none;border-radius:8px;"></iframe>'
    except Exception:
        return f'<div style="height:{height}px;display:flex;align-items:center;justify-content:center;color:#64748b;">Chart unavailable.</div>'


def _map_error_html(height: int, msg: str = "Map unavailable.") -> str:
    return (
        f'<div style="height:{height}px;display:flex;align-items:center;'
        f'justify-content:center;color:#64748b;border-radius:12px;'
        f'background:#f8fafc;border:1px solid #e2e8f0;">{msg}</div>'
    )


def deck_to_iframe(deck: pdk.Deck, height: int = 480) -> str:
    try:
        # pydeck versions differ: some support iframe_height/iframe_width, some don't.
        # Try newer signature first, then gracefully fall back for older deployments.
        try:
            raw = deck.to_html(as_string=True, iframe_height=height, iframe_width="100%")
        except TypeError:
            raw = deck.to_html(as_string=True)
        b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        return f'<iframe src="data:text/html;base64,{b64}" width="100%" height="{height}" style="border:none;border-radius:12px;"></iframe>'
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"[:180]
        return _map_error_html(height, f"Map unavailable: {html.escape(err)}")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

app_ui = ui.page_fluid(
    ui.tags.head(
        ui.tags.link(rel="icon", href=FAVICON_SVG, type="image/svg+xml"),
        ui.tags.style("""
        body { font-family: 'Georgia', serif; background: #f8fafc; color: #1e293b; overflow-x: hidden; }
        .navbar-custom { background: #1e293b; padding: 0.75rem 1.5rem; }
        .navbar-custom .brand { font-size: 1.15rem; font-weight: 600; color: #f1f5f9; letter-spacing: 0.01em; }
        .navbar-custom .sub { font-size: 0.8rem; color: #94a3b8; margin-left: 1rem; }
        .dashboard-card { background: white; border-radius: 12px; border: 1px solid #e2e8f0; padding: 1.25rem; margin-bottom: 1rem; }
        .metric-cards-row { align-items: stretch; }
        .metric-cards-row > .col { display: flex; min-width: 0; }
        .metric-card { background: #f8fafc; border-radius: 10px; border: 1px solid #e2e8f0; padding: 1rem 1.1rem; text-align: center;
          flex: 1 1 auto; width: 100%; min-height: 8.75rem; box-sizing: border-box;
          display: flex; flex-direction: column; justify-content: center; align-items: center; }
        @media (max-width: 991.98px) {
          .metric-cards-row > .col { flex: 0 0 50%; max-width: 50%; }
        }
        @media (max-width: 575.98px) {
          .metric-cards-row > .col { flex: 0 0 100%; max-width: 100%; }
        }
        .metric-value { font-size: 1.9rem; font-weight: 700; color: #0f172a; font-family: 'Georgia', serif; }
        .metric-value.positive { color: #0f6e56; }
        .metric-value.negative { color: #993C1D; }
        .metric-label { font-size: 0.72rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.3rem; }
        .metric-sub { font-size: 0.8rem; color: #94a3b8; margin-top: 0.15rem; }
        .policy-badge { display: inline-block; padding: 0.2rem 0.65rem; border-radius: 20px; font-size: 0.75rem; font-weight: 600; }
        .badge-treated { background: #e1f5ee; color: #0f6e56; }
        .badge-untreated { background: #f1efe8; color: #5f5e5a; }
        .section-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; color: #94a3b8; margin-bottom: 0.5rem; }
        .tab-content { padding-top: 1rem; }
        hr { border-color: #e2e8f0; }
        """),
    ),
    ui.tags.nav(
        {"class": "navbar-custom"},
        ui.tags.div(
            {"class": "d-flex align-items-center"},
            ui.tags.span({"class": "brand"}, "Congestion Pricing Research Dashboard"),
            ui.tags.span({"class": "sub"}, "Single-city analysis · reference context"),
        ),
    ),
    ui.layout_sidebar(
        ui.sidebar(
            ui.tags.div({"class": "section-label"}, "Analysis scope"),
            ui.input_select(
                "primary_city",
                "Primary city (charts & tables)",
                choices={"SGP": "Singapore", "LON": "London"},
                selected="SGP",
            ),
            ui.input_checkbox_group(
                "reference_cities",
                "Reference cities (context for AI only; not shown in charts)",
                choices={"SGP": "Singapore", "LON": "London"},
                selected=[],
            ),
            ui.tags.hr(),
            ui.tags.div({"class": "section-label mt-2"}, "Metric"),
            ui.input_select(
                "metric_type",
                None,
                choices={"": "All metrics"},
                selected="",
            ),
            ui.tags.hr(),
            ui.tags.div({"class": "section-label mt-2"}, "Year range"),
            ui.input_numeric("year_min", "From", value=1990, min=1975, max=2024),
            ui.input_numeric("year_max", "To", value=2024, min=1975, max=2024),
            ui.tags.hr(),
            ui.input_action_button("load_btn", "Load data", class_="btn-primary w-100 mt-1"),
            ui.tags.div(
                {"class": "mt-2 p-2", "style": "min-height:2.5rem;border:1px solid #e2e8f0;border-radius:8px;background:white;"},
                ui.output_ui("loading_status_ui"),
            ),
            title="Filters",
            width=240,
        ),
        ui.div(
            {"style": "padding: 1.25rem;"},
            # Metric cards row (equal width columns, shared min-height)
            ui.div(
                {"class": "row g-2 metric-cards-row"},
                ui.div({"class": "col"}, ui.div({"class": "metric-card"}, ui.output_ui("card_cities"))),
                ui.div({"class": "col"}, ui.div({"class": "metric-card"}, ui.output_ui("card_latest_value"))),
                ui.div({"class": "col"}, ui.div({"class": "metric-card"}, ui.output_ui("card_recent_avg"))),
                ui.div({"class": "col"}, ui.div({"class": "metric-card"}, ui.output_ui("card_recent_trend"))),
                ui.div({"class": "col"}, ui.div({"class": "metric-card"}, ui.output_ui("card_volatility"))),
            ),
            ui.tags.hr(),
            # Tabs
            ui.navset_tab(
                # ---- Tab 1: Policy information ----
                ui.nav_panel(
                    "Policy information",
                    ui.row(
                        ui.column(
                            7,
                            ui.div(
                                {"class": "dashboard-card"},
                                ui.tags.h5("Primary city location", {"style": "font-size:0.95rem;font-weight:600;margin-bottom:0.75rem;"}),
                                ui.output_ui("map_ui"),
                            ),
                        ),
                        ui.column(
                            5,
                            ui.div(
                                {"class": "dashboard-card"},
                                ui.tags.h5("Annual mean (last 3 years)", {"style": "font-size:0.95rem;font-weight:600;margin-bottom:0.75rem;"}),
                                ui.output_ui("recent_years_bar_chart_ui"),
                            ),
                        ),
                    ),
                    ui.row(
                        ui.column(
                            12,
                            ui.div(
                                {"class": "dashboard-card"},
                                ui.tags.h5("Policy periods (primary city)", {"style": "font-size:0.95rem;font-weight:600;margin-bottom:0.75rem;"}),
                                ui.output_ui("policy_table_ui"),
                            ),
                        ),
                    ),
                ),
                # ---- Tab 2: Trending ----
                ui.nav_panel(
                    "Trending",
                    ui.row(
                        ui.column(
                            12,
                            ui.div(
                                {"class": "dashboard-card"},
                                ui.tags.h5("Primary city: values over time", {"style": "font-size:0.95rem;font-weight:600;margin-bottom:0.75rem;"}),
                                ui.output_ui("trend_chart_ui"),
                            ),
                        ),
                    ),
                    ui.row(
                        ui.column(
                            12,
                            ui.div(
                                {"class": "dashboard-card"},
                                ui.tags.h5("Data table (primary city)", {"style": "font-size:0.95rem;font-weight:600;margin-bottom:0.75rem;"}),
                                ui.output_ui("data_table_ui"),
                            ),
                        ),
                    ),
                ),
            ),
            # AI Summary (below tabs, always visible)
            ui.tags.hr(),
            ui.div(
                {"class": "dashboard-card"},
                ui.tags.h5("AI policy recommendations", {"style": "font-size:0.95rem;font-weight:600;margin-bottom:0.75rem;"}),
                ui.input_action_button("ai_btn", "Generate recommendations", class_="btn-primary mb-2"),
                ui.output_ui("ai_summary_ui"),
            ),
        ),
    ),
    title="Congestion Pricing Research Dashboard",
)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

def server(input, output, session):

    # Reactive state
    obs_primary      = reactive.Value(pd.DataFrame())
    obs_reference    = reactive.Value(pd.DataFrame())
    cities_df        = reactive.Value(pd.DataFrame())
    policy_primary_df = reactive.Value(pd.DataFrame())
    policy_reference_df = reactive.Value(pd.DataFrame())
    metric_units_df  = reactive.Value(pd.DataFrame())
    loading        = reactive.Value(False)
    load_error     = reactive.Value(None)
    ai_text        = reactive.Value("")
    ai_loading     = reactive.Value(False)

    @reactive.Effect
    @reactive.event(input.primary_city)
    def _metric_choices_for_primary_city():
        """Metric dropdown lists only metric_types that appear in data for the primary city."""
        try:
            mu = fetch_metric_units()
            metric_units_df.set(mu)
        except Exception:
            mu = pd.DataFrame()
        primary = (input.primary_city() or "").strip()
        types: list[str] = []
        if primary:
            try:
                types = fetch_metric_types_for_city(primary)
            except Exception:
                types = []
        name_map: dict[str, str] = {}
        if not mu.empty and "metric_type" in mu.columns and "display_name" in mu.columns:
            name_map = dict(zip(mu["metric_type"].astype(str), mu["display_name"].astype(str)))
        choices: dict[str, str] = {"": "All metrics"}
        for t in types:
            choices[t] = name_map.get(t, str(t).replace("_", " "))
        curr = input.metric_type()
        selected = curr if curr in choices else ""
        ui.update_select("metric_type", choices=choices, selected=selected, session=session)

    @reactive.Effect
    @reactive.event(input.primary_city, input.reference_cities)
    def _sync_reference_cities():
        """Keep only Singapore/London, and never duplicate the primary city as a reference."""
        p = input.primary_city()
        allowed = {"SGP", "LON"}
        curr = list(input.reference_cities() or [])
        new = [c for c in curr if c in allowed and c != p]
        if new != curr:
            ui.update_checkbox_group("reference_cities", selected=new, session=session)

    # -----------------------------------------------------------------------
    # Data loading
    # -----------------------------------------------------------------------

    def _do_load():
        loading.set(True)
        load_error.set(None)
        try:
            cities = fetch_cities()
            cities_df.set(cities)

            primary = input.primary_city()
            refs = [c for c in (input.reference_cities() or []) if c and c != primary]

            if not primary:
                obs_primary.set(pd.DataFrame())
                obs_reference.set(pd.DataFrame())
                policy_primary_df.set(pd.DataFrame())
                policy_reference_df.set(pd.DataFrame())
                loading.set(False)
                return

            y0 = int(input.year_min() or 1975)
            y1 = int(input.year_max() or 2024)
            mt = input.metric_type() or None

            obs_p = fetch_observations(
                city_codes=[primary],
                metric_type=mt,
                year_min=y0,
                year_max=y1,
            )
            obs_primary.set(obs_p)

            if refs:
                obs_r = fetch_observations(
                    city_codes=refs,
                    metric_type=mt,
                    year_min=y0,
                    year_max=y1,
                )
                obs_reference.set(obs_r)
            else:
                obs_reference.set(pd.DataFrame())

            pp = fetch_policy_periods(city_code=primary)
            if not pp.empty:
                city_match = cities[cities["city_code"] == primary]
                pp = pp.copy()
                pp["city_code"] = primary
                pp["city_name"] = city_match.iloc[0]["city_name"] if not city_match.empty else primary
            policy_primary_df.set(pp)

            ref_pol: list[pd.DataFrame] = []
            for code in refs:
                p = fetch_policy_periods(city_code=code)
                if not p.empty:
                    city_match = cities[cities["city_code"] == code]
                    p = p.copy()
                    p["city_code"] = code
                    p["city_name"] = city_match.iloc[0]["city_name"] if not city_match.empty else code
                    ref_pol.append(p)
            policy_reference_df.set(
                pd.concat(ref_pol, ignore_index=True) if ref_pol else pd.DataFrame()
            )

        except Exception as e:
            load_error.set(str(e)[:200])
            obs_primary.set(pd.DataFrame())
            obs_reference.set(pd.DataFrame())
            policy_primary_df.set(pd.DataFrame())
            policy_reference_df.set(pd.DataFrame())
        finally:
            loading.set(False)

    @reactive.Effect
    @reactive.event(input.load_btn)
    def _on_load():
        _do_load()

    # -----------------------------------------------------------------------
    # Derived reactives
    # -----------------------------------------------------------------------

    @reactive.Calc
    def monitoring_metrics():
        df = obs_primary.get()
        if df.empty:
            return {}
        return _monitoring_metrics(df)

    # -----------------------------------------------------------------------
    # Metric cards
    # -----------------------------------------------------------------------

    def _fmt_k(v: Optional[float]) -> str:
        """Round to nearest thousand; display as e.g. 293218 → 293k."""
        if v is None:
            return "—"
        if isinstance(v, float) and np.isnan(v):
            return "—"
        k = int(round(float(v) / 1000.0))
        return f"{k}k"

    @render.ui
    def card_cities():
        s = monitoring_metrics()
        name = s.get("primary_name") or s.get("primary_code") or "—"
        return ui.div(
            ui.div(str(name), {"class": "metric-value", "style": "font-size:1.35rem;"}),
            ui.div("Primary city", {"class": "metric-label"}),
            ui.div(f"{s.get('n_obs', 0):,} observations", {"class": "metric-sub"}),
        )

    @render.ui
    def card_latest_value():
        s = monitoring_metrics()
        if s.get("mixed_metrics"):
            return ui.div(
                ui.div("—", {"class": "metric-value"}),
                ui.div("Latest (annual mean)", {"class": "metric-label"}),
                ui.div("Select one metric", {"class": "metric-sub"}),
            )
        lv = s.get("latest_value")
        ly = s.get("latest_year")
        return ui.div(
            ui.div(_fmt_k(lv), {"class": "metric-value"}),
            ui.div("Latest annual mean", {"class": "metric-label"}),
            ui.div(f"{ly} · {s.get('unit', '')}" if ly else s.get("unit", ""), {"class": "metric-sub"}),
        )

    @render.ui
    def card_recent_avg():
        s = monitoring_metrics()
        if s.get("mixed_metrics"):
            return ui.div(
                ui.div("—", {"class": "metric-value"}),
                ui.div("Recent 3-yr mean", {"class": "metric-label"}),
                ui.div("Select one metric", {"class": "metric-sub"}),
            )
        v = s.get("recent_avg")
        sub = f"Mean of up to 3 most recent years · {s.get('unit', '')}"
        return ui.div(
            ui.div(_fmt_k(v), {"class": "metric-value"}),
            ui.div("Recent 3-year average", {"class": "metric-label"}),
            ui.div(sub, {"class": "metric-sub"}),
        )

    @render.ui
    def card_recent_trend():
        s = monitoring_metrics()
        if s.get("mixed_metrics"):
            return ui.div(
                ui.div("—", {"class": "metric-value"}),
                ui.div("3-year trend", {"class": "metric-label"}),
                ui.div("Select one metric", {"class": "metric-sub"}),
            )
        pct = s.get("recent_trend_pct")
        ly = s.get("latest_year")
        if pct is None:
            val_html = ui.div("—", {"class": "metric-value"})
            sub = "Needs year (latest−3) in range" if ly else ""
        elif pct < 0:
            val_html = ui.div(f"{pct:+.1f}%", {"class": "metric-value negative"})
            sub = f"Latest year vs {int(ly) - 3}" if ly else "vs 3 years prior"
        else:
            val_html = ui.div(f"{pct:+.1f}%", {"class": "metric-value positive"})
            sub = f"Latest year vs {int(ly) - 3}" if ly else "vs 3 years prior"
        return ui.div(
            val_html,
            ui.div("Change vs 3 years prior", {"class": "metric-label"}),
            ui.div(sub, {"class": "metric-sub"}),
        )

    @render.ui
    def card_volatility():
        s = monitoring_metrics()
        if s.get("mixed_metrics"):
            return ui.div(
                ui.div("—", {"class": "metric-value"}),
                ui.div("Volatility", {"class": "metric-label"}),
                ui.div("Select one metric", {"class": "metric-sub"}),
            )
        label = s.get("volatility_label")
        std = s.get("volatility_std")
        if not label:
            return ui.div(
                ui.div("—", {"class": "metric-value"}),
                ui.div("Volatility (5-yr)", {"class": "metric-label"}),
                ui.div("Need 2+ annual points", {"class": "metric-sub"}),
            )
        return ui.div(
            ui.div(label.replace(" variability", ""), {"class": "metric-value", "style": "font-size:1.45rem;"}),
            ui.div("Variability (5-yr annual)", {"class": "metric-label"}),
            ui.div(
                f"{label} · σ≈{_fmt_k(std)}" if std is not None else label,
                {"class": "metric-sub"},
            ),
        )

    # -----------------------------------------------------------------------
    # Loading status
    # -----------------------------------------------------------------------

    @render.ui
    def loading_status_ui():
        err = load_error.get()
        if err:
            return ui.div(err, {"style": "font-size:0.8rem;color:#a32d2d;"})
        if loading():
            return ui.div(
                ui.HTML('<span class="spinner-border spinner-border-sm text-primary me-2"></span>'),
                "Loading…",
                {"style": "font-size:0.85rem;color:#64748b;"},
            )
        df = obs_primary.get()
        if df.empty:
            return ui.div("Select a primary city and click Load data.", {"style": "font-size:0.8rem;color:#94a3b8;"})
        return ui.div(
            f"{len(df):,} rows loaded",
            {"style": "font-size:0.8rem;color:#0f6e56;font-weight:500;"},
        )

    # -----------------------------------------------------------------------
    # Map
    # -----------------------------------------------------------------------

    @render.ui
    def map_ui():
        if loading():
            return ui.HTML(_map_error_html(480, "Loading…"))
        cities = cities_df.get()
        obs = obs_primary.get()
        primary = input.primary_city()
        if not primary:
            return ui.HTML(_map_error_html(480, "Choose a primary city and load data."))
        if cities.empty:
            return ui.HTML(_map_error_html(480, "Load data to see the map."))
        cities = cities[cities["city_code"] == primary]
        try:
            deck = build_city_map(cities, obs if not obs.empty else None)
            return ui.HTML(deck_to_iframe(deck, height=480))
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)}"[:180]
            return ui.HTML(_map_error_html(480, f"Map error: {html.escape(err)}"))

    # -----------------------------------------------------------------------
    # Annual mean bar chart (last 3 years)
    # -----------------------------------------------------------------------

    @render.ui
    def recent_years_bar_chart_ui():
        if loading():
            return ui.HTML('<div style="height:420px;"></div>')
        df = obs_primary.get()
        if df.empty:
            return ui.div("Load data to see annual means.", {"style": "color:#64748b;padding:2rem;text-align:center;"})

        am = _annual_means_for_chart(df)
        if am.empty:
            return ui.div(
                "Select a single metric to show mean vehicle counts by year.",
                {"style": "color:#64748b;padding:2rem;text-align:center;"},
            )

        last3 = am.tail(3)
        code = input.primary_city()
        color = CITY_PALETTE.get(code, "#64748b")
        unit = df["unit"].iloc[0] if "unit" in df.columns else ""

        fig = go.Figure(
            data=[
                go.Bar(
                    x=[str(int(y)) for y in last3.index],
                    y=last3.values,
                    marker_color=color,
                    text=[f"{float(v):,.0f}" for v in last3.values],
                    textposition="outside",
                    hovertemplate=(
                        "<b>Year %{x}</b><br>"
                        "Annual mean: %{y:,.0f} "
                        + (str(unit) if unit else "")
                        + "<extra></extra>"
                    ),
                )
            ]
        )
        fig.update_layout(
            height=420,
            showlegend=False,
            margin=dict(l=48, r=20, t=24, b=48),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#f8fafc",
            yaxis=dict(
                title=f"Mean value ({unit})" if unit else "Mean value",
                showgrid=True,
                gridcolor="#e2e8f0",
                gridwidth=1,
                zeroline=True,
                zerolinecolor="#cbd5e1",
            ),
            xaxis=dict(
                title="Calendar year",
                showgrid=True,
                gridcolor="#e2e8f0",
                type="category",
            ),
            bargap=0.35,
        )
        return ui.HTML(_plotly_to_iframe_html(fig, height=420, div_id="recent-yrs-chart"))

    # -----------------------------------------------------------------------
    # Policy periods table
    # -----------------------------------------------------------------------

    @render.ui
    def policy_table_ui():
        df = policy_primary_df.get()
        if df.empty:
            return ui.div("Load data to see policy periods.", {"style": "color:#64748b;padding:1rem;"})

        rows = []
        for _, row in df.iterrows():
            badge_cls = "badge-treated" if row.get("treated") else "badge-untreated"
            badge_txt = "Treated" if row.get("treated") else "Pre-policy"
            start = str(row["start_date"])[:10] if pd.notna(row.get("start_date")) else "—"
            end   = str(row["end_date"])[:10]   if pd.notna(row.get("end_date"))   else "Ongoing"
            rows.append(
                f"<tr>"
                f"<td>{row.get('city_name','')}</td>"
                f"<td><code style='font-size:0.8rem'>{row.get('policy_uid','')}</code></td>"
                f"<td>{row.get('label','')}</td>"
                f"<td>{row.get('system_type','—')}</td>"
                f"<td>{start}</td>"
                f"<td>{end}</td>"
                f"<td><span class='policy-badge {badge_cls}'>{badge_txt}</span></td>"
                f"</tr>"
            )

        thead = (
            "<thead style='font-size:0.78rem;text-transform:uppercase;color:#64748b;'>"
            "<tr><th>City</th><th>Policy UID</th><th>Label</th><th>System type</th>"
            "<th>Start</th><th>End</th><th>Status</th></tr></thead>"
        )
        return ui.HTML(
            f'<div style="overflow-x:auto;">'
            f'<table class="table table-sm" style="font-size:0.88rem;">'
            f'{thead}<tbody>{"".join(rows)}</tbody>'
            f'</table></div>'
        )

    # -----------------------------------------------------------------------
    # Trend line chart
    # -----------------------------------------------------------------------

    @render.ui
    def trend_chart_ui():
        if loading():
            return ui.HTML(f'<div style="height:420px;"></div>')
        df = obs_primary.get()
        if df.empty:
            return ui.div("Load data to see trends.", {"style": "color:#64748b;padding:2rem;text-align:center;"})

        fig = go.Figure()
        multi_metric = "metric_type" in df.columns and df["metric_type"].nunique() > 1
        y_label = "Value" if multi_metric else "Vehicle count"

        x_kind_used: Optional[str] = None
        trace_idx = 0
        x_mins: list[float] = []
        x_maxs: list[float] = []

        def _add_trend_trace(city_df: pd.DataFrame, code: str, trace_name: str, metric_type: Optional[str]) -> None:
            nonlocal x_kind_used, trace_idx
            if city_df.empty:
                return
            city_df = _aggregate_quarterly_mean(city_df)
            if city_df.empty:
                return
            d, xv, x_kind = _prepare_trend_plot_data(city_df)
            x_kind_used = x_kind
            color = _line_color_for_metric(metric_type, code, trace_idx)
            trace_idx += 1

            if hasattr(xv, "min"):
                x_mins.append(float(pd.Series(xv).min()))
                x_maxs.append(float(pd.Series(xv).max()))
            else:
                x_mins.append(float(np.min(xv)))
                x_maxs.append(float(np.max(xv)))

            customdata = None
            if x_kind == "quarter_float" and "obs_year" in d.columns and "obs_quarter" in d.columns:
                customdata = np.column_stack(
                    [d["obs_year"].values, d["obs_quarter"].fillna(0).values]
                )
                hovertemplate = (
                    "<b>" + trace_name + "</b><br>"
                    "Year %{customdata[0]:.0f} Q%{customdata[1]:.0f} (mean)<br>"
                    "Value: %{y:,.0f}<extra></extra>"
                )
            elif x_kind == "datetime":
                hovertemplate = (
                    "<b>" + trace_name + "</b><br>"
                    "%{x|%Y-%m-%d}<br>"
                    "Value: %{y:,.0f}<extra></extra>"
                )
            else:
                hovertemplate = (
                    "<b>" + trace_name + "</b><br>"
                    "Time: %{x}<br>"
                    "Value: %{y:,.0f}<extra></extra>"
                )

            fig.add_trace(
                go.Scatter(
                    x=xv,
                    y=d["value"],
                    mode="lines+markers",
                    name=trace_name,
                    line=dict(color=color, width=2.5),
                    marker=dict(size=6, color=color),
                    customdata=customdata,
                    hovertemplate=hovertemplate,
                )
            )

        if multi_metric:
            for (code, mt), city_df in df.groupby(["city_code", "metric_type"]):
                base_name = city_df.iloc[0]["city_name"]
                label_mt = str(mt).replace("_", " ")
                trace_name = f"{base_name} ({label_mt})"
                _add_trend_trace(city_df, code, trace_name, str(mt))
        else:
            for code in df["city_code"].unique():
                city_df = df[df["city_code"] == code]
                name = city_df.iloc[0]["city_name"]
                mt = str(city_df.iloc[0]["metric_type"]) if "metric_type" in city_df.columns else None
                _add_trend_trace(city_df, code, name, mt)

        policies = policy_primary_df.get()
        if not policies.empty and x_kind_used:
            y_max_year = int(df["obs_year"].max()) if "obs_year" in df.columns else None
            for code in df["city_code"].unique():
                base_color = CITY_PALETTE.get(code, "#888780")
                city_policies = policies[(policies["city_code"] == code) & (policies["treated"] == True)]
                for _, p in city_policies.iterrows():
                    p_start = p["start_date"].year if pd.notna(p.get("start_date")) else None
                    p_end = p["end_date"].year if pd.notna(p.get("end_date")) else None
                    if p_start is None:
                        continue
                    if x_kind_used == "datetime":
                        x0 = datetime.datetime(int(p_start), 1, 1)
                        if p_end is not None:
                            x1 = datetime.datetime(int(p_end), 12, 31)
                        else:
                            x1 = pd.Timestamp.now()
                        fig.add_vrect(
                            x0=x0,
                            x1=x1,
                            fillcolor=base_color,
                            opacity=0.07,
                            layer="below",
                            line_width=0,
                        )
                    else:
                        x0f = float(p_start)
                        if p_end is not None:
                            x1f = float(p_end) + 1.0
                        elif y_max_year is not None:
                            x1f = float(y_max_year) + 1.0
                        else:
                            x1f = x0f + 1.0
                        fig.add_vrect(
                            x0=x0f,
                            x1=x1f,
                            fillcolor=base_color,
                            opacity=0.07,
                            layer="below",
                            line_width=0,
                        )

        # Vertical lines at policy start (DB flag chart_mark_start) — e.g. London ULEZ expansion
        if not policies.empty and x_kind_used:
            for code in df["city_code"].unique():
                for _, p in policies[policies["city_code"] == code].iterrows():
                    mark = p.get("chart_mark_start")
                    if mark is None or (
                        isinstance(mark, (float, np.floating)) and np.isnan(mark)
                    ):
                        continue
                    if not mark:
                        continue
                    sd = p.get("start_date")
                    if sd is None or (isinstance(sd, float) and np.isnan(sd)):
                        continue
                    ts = pd.Timestamp(sd)
                    xv = _policy_date_to_trend_x(ts, x_kind_used)
                    label = str(p.get("label") or "Policy start")[:52]
                    fig.add_vline(
                        x=xv,
                        line_width=2,
                        line_color="#b45309",
                        annotation_text=label,
                        annotation_position="top",
                        layer="above",
                    )

        xmin = min(x_mins) if x_mins else None
        xmax = max(x_maxs) if x_maxs else None

        if x_kind_used == "datetime":
            xaxis = dict(
                title="Time",
                type="date",
                tickformat="%Y",
                dtick="M12",
                showgrid=True,
                gridcolor="#e2e8f0",
                gridwidth=1,
                zeroline=False,
            )
        elif x_kind_used == "quarter_float" and xmin is not None:
            t0 = float(np.floor(xmin))
            t1 = float(np.ceil(xmax if xmax is not None else xmin))
            tick_vals = [y for y in range(int(t0), int(t1) + 1)]
            xaxis = dict(
                title="Time (quarterly mean; ticks are calendar years)",
                tickmode="array",
                tickvals=tick_vals,
                ticktext=[str(y) for y in tick_vals],
                showgrid=True,
                gridcolor="#e2e8f0",
                gridwidth=1,
                zeroline=False,
            )
        else:
            xaxis = dict(
                title="Year",
                showgrid=True,
                gridcolor="#e2e8f0",
                gridwidth=1,
                dtick=1,
                zeroline=False,
            )

        # Policy vrects can start years before the series; Plotly then autoranges the x-axis
        # to include those shapes and leaves a large empty margin. Pin the axis to data extent.
        if x_kind_used == "datetime" and fig.data:
            tmin: Optional[pd.Timestamp] = None
            tmax: Optional[pd.Timestamp] = None
            for tr in fig.data:
                tx = getattr(tr, "x", None)
                if tx is None or len(tx) == 0:
                    continue
                tss = pd.to_datetime(pd.Series(tx).dropna())
                if len(tss) == 0:
                    continue
                lo, hi = tss.min(), tss.max()
                tmin = lo if tmin is None else min(tmin, lo)
                tmax = hi if tmax is None else max(tmax, hi)
            if tmin is not None and tmax is not None:
                pad = pd.Timedelta(days=75)
                xaxis["range"] = [tmin - pad, tmax + pad]
        elif x_kind_used in ("quarter_float", "year_float") and xmin is not None and xmax is not None:
            span = max(float(xmax) - float(xmin), 1e-6)
            pad = max(0.06 * span, 0.12)
            xaxis["range"] = [float(xmin) - pad, float(xmax) + pad]

        yaxis = dict(
            title=y_label,
            showgrid=True,
            gridcolor="#e2e8f0",
            gridwidth=1,
            zeroline=True,
            zerolinecolor="#cbd5e1",
            zerolinewidth=1,
        )

        fig.update_layout(
            height=420,
            margin=dict(l=50, r=20, t=30, b=50),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#f8fafc",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=xaxis,
            yaxis=yaxis,
            hovermode="closest",
        )

        return ui.HTML(_plotly_to_iframe_html(fig, height=420, div_id="trend-chart"))

    # -----------------------------------------------------------------------
    # Data table (Trends tab)
    # -----------------------------------------------------------------------

    @render.ui
    def data_table_ui():
        df = obs_primary.get()
        if df.empty:
            return ui.div("No data loaded.", {"style": "color:#64748b;padding:1rem;"})

        display_cols = [
            "city_code", "city_name", "obs_year", "obs_quarter",
            "metric_type", "value", "unit", "temporal_grain",
            "policy_label", "treated", "source_api",
        ]
        show = [c for c in display_cols if c in df.columns]
        top = df[show].sort_values(["city_code", "obs_year"]).head(100)

        thead_cells = "".join(
            f"<th>{c.replace('_', ' ').title()}</th>" for c in show
        )
        thead = (
            f"<thead style='font-size:0.75rem;text-transform:uppercase;color:#64748b;'>"
            f"<tr>{thead_cells}</tr></thead>"
        )

        rows = []
        for _, row in top.iterrows():
            cells = []
            for c in show:
                v = row[c]
                if c == "treated":
                    badge = "badge-treated" if v else "badge-untreated"
                    txt   = "Yes" if v else "No"
                    cells.append(f"<td><span class='policy-badge {badge}'>{txt}</span></td>")
                elif c == "value" and pd.notna(v):
                    cells.append(f"<td>{int(v):,}</td>")
                elif pd.isna(v) if not isinstance(v, str) else False:
                    cells.append("<td style='color:#94a3b8'>—</td>")
                else:
                    cells.append(f"<td>{v}</td>")
            rows.append(f"<tr style='font-size:0.85rem;'>{''.join(cells)}</tr>")

        return ui.HTML(
            f'<div style="overflow-x:auto;max-height:340px;overflow-y:auto;">'
            f'<table class="table table-sm table-hover">'
            f'{thead}<tbody>{"".join(rows)}</tbody>'
            f'</table>'
            f'<p style="font-size:0.75rem;color:#94a3b8;margin-top:0.5rem;">Showing first 100 rows.</p>'
            f'</div>'
        )

    # -----------------------------------------------------------------------
    # AI summary
    # -----------------------------------------------------------------------

    SYSTEM_PROMPT = """You are an expert in transportation and congestion pricing policy advising researchers and decision-makers.

Output must be exactly three paragraphs of continuous academic prose. No markdown, no bullet points, no headings, no numbered lists.

Paragraph 1: Identify and interpret the primary city's latest policy (from the structured policy line provided). Discuss whether the monitoring signals are plausibly consistent with an impact around or after that policy's start, or whether trends appear ambiguous or dominated by other factors. Be explicit that vehicle-count series alone do not prove causation; speak in terms of consistency, timing, and uncertainty.

Paragraph 2: Summarize directional patterns and policy timing for each reference jurisdiction using the monitoring signals provided. Do not rank cities and do not compare absolute levels across cities.

Paragraph 3: Give recommended next steps for the primary city: whether further policy action (e.g. additional scheme, expansion, or tightening) appears warranted, premature, or should wait pending better evidence—justify using the same signals and limitations. If the evidence is insufficient to decide, say so and name what would be needed (e.g. longer post-implementation window, disaggregated metrics). Acknowledge data limitations (metric coverage, aggregation, missing years).

Constraints:
- Do not compare raw numeric levels across cities or imply that one city's counts are comparable to another's.
- Use percent changes and volatility labels only as within-jurisdiction directional evidence.
- Tone: academic, concise, evidence-based.
- About 380 words maximum."""

    @reactive.Effect
    @reactive.event(input.ai_btn)
    def _run_ai():
        ai_loading.set(True)
        ai_text.set("")
        ui.update_action_button("ai_btn", disabled=True, session=session)
        try:
            from llm_cloud import query_llm, OllamaCloudError
        except ImportError:
            ai_text.set("AI recommendations unavailable: llm_cloud module not found.")
            ai_loading.set(False)
            ui.update_action_button("ai_btn", disabled=False, session=session)
            return

        try:
            primary_code = input.primary_city()
            df_p = obs_primary.get()
            df_r = obs_reference.get()
            pol_p = policy_primary_df.get()
            pol_r = policy_reference_df.get()
            m_sel = input.metric_type()
            metric_label = "all metrics selected (multiple series may differ in scale)" if not m_sel else str(m_sel)
            y_range = (
                f"{int(df_p['obs_year'].min())}–{int(df_p['obs_year'].max())}"
                if not df_p.empty and "obs_year" in df_p.columns
                else "N/A"
            )

            p_m = _monitoring_metrics(df_p)
            p_trend = _trend_direction(df_p)
            p_timing = _policy_timing_treated(pol_p, primary_code)
            p_latest_pol = _latest_policy_brief(pol_p, primary_code)

            primary_lines: list[str] = [
                f"city_code: {primary_code}",
                f"metric_filter: {metric_label}",
                f"year_range_in_data: {y_range}",
                f"latest_policy_to_analyze: {p_latest_pol}",
                f"policy_periods_treated_calendar_years: {p_timing}",
                f"annual_series_trend_direction: {p_trend}",
            ]
            if p_m.get("mixed_metrics"):
                primary_lines.append("monitoring_metrics: mixed_metrics (select one metric for numeric signals)")
            else:
                primary_lines.extend(
                    [
                        f"latest_annual_mean_value: {p_m.get('latest_value')}, year: {p_m.get('latest_year')}",
                        f"mean_of_last_3_years_with_data: {p_m.get('recent_avg')}",
                        f"pct_change_latest_year_vs_year_minus_3: {p_m.get('recent_trend_pct')}",
                        f"volatility_band_5yr_annual_means: {p_m.get('volatility_label')}",
                        f"unit: {p_m.get('unit')}",
                    ]
                )

            ref_blocks: list[str] = []
            if df_r.empty or "city_code" not in df_r.columns:
                ref_blocks.append("No reference data loaded.")
            else:
                for code in df_r["city_code"].dropna().unique():
                    rdf = df_r[df_r["city_code"] == code]
                    name = rdf.iloc[0]["city_name"] if "city_name" in rdf.columns else code
                    rm = _monitoring_metrics(rdf)
                    tr = _trend_direction(rdf)
                    tim = _policy_timing_treated(pol_r, str(code))
                    ref_lines = [
                        f"reference_city: {name} ({code})",
                        f"policy_periods_treated_calendar_years: {tim}",
                        f"annual_series_trend_direction: {tr}",
                    ]
                    if rm.get("mixed_metrics"):
                        ref_lines.append("monitoring_metrics: mixed_metrics")
                    else:
                        ref_lines.extend(
                            [
                                f"latest_annual_mean_value: {rm.get('latest_value')}, year: {rm.get('latest_year')}",
                                f"mean_of_last_3_years: {rm.get('recent_avg')}",
                                f"pct_change_latest_vs_year_minus_3: {rm.get('recent_trend_pct')}",
                                f"volatility_band: {rm.get('volatility_label')}",
                                f"unit: {rm.get('unit')}",
                            ]
                        )
                    ref_blocks.append("\n".join(ref_lines))

            user_prompt = f"""Structured signals for policy recommendations (single-city primary analysis).

=== PRIMARY CITY (charts and official statistics refer to this city only) ===
{chr(10).join(primary_lines)}

=== REFERENCE JURISDICTIONS (context only; do not treat as benchmarks for absolute levels) ===
{chr(10).join(ref_blocks)}

Instructions: Write the three paragraphs specified in the system prompt. Center paragraph 1 on the latest_policy_to_analyze line together with the monitoring metrics. In paragraph 3, address explicitly whether implementing another policy (or expanding or tightening an existing one) is warranted, not warranted, or unclear—and why. Reference blocks use parallel within-city statistics; you must not compare raw values across cities or claim one city is "better" than another on the basis of these numbers."""

            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ]
            out = query_llm(messages, stream=False)
            ai_text.set(sanitize_report_text(out) if out else "Recommendations unavailable.")
        except Exception as e:
            ai_text.set(f"Recommendations unavailable. ({str(e)[:120]})")
        finally:
            ai_loading.set(False)
            ui.update_action_button("ai_btn", disabled=False, session=session)

    @render.ui
    def ai_summary_ui():
        if ai_loading.get():
            return ui.div(
                ui.HTML('<span class="spinner-border spinner-border-sm text-primary me-2"></span>'),
                "Generating recommendations…",
                {"style": "color:#64748b;padding:0.5rem 0;font-size:0.9rem;"},
            )
        text = ai_text.get()
        if not text:
            return ui.div(
                "Click 'Generate recommendations' for analysis of the latest policy, plausible impacts, and next steps on further policy action.",
                {"style": "color:#94a3b8;font-size:0.88rem;padding:0.5rem 0;"},
            )
        s = (text
             .replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace("\n\n", "<br/><br/>")
             .replace("\n", "<br/>"))
        return ui.div(
            ui.HTML(
                f'<div style="max-height:240px;overflow-y:auto;padding:0.5rem 0;'
                f'font-size:0.9rem;line-height:1.7;color:#1e293b;">{s}</div>'
            )
        )


app = App(app_ui, server)
