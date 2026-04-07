"""
Bar Harbor Congestion Intelligence Dashboard — Shiny for Python.
Includes visualization-layer debug: len(map_df), sample path geometry, gauge value.
PyDeck PathLayer expects path = list of [lon, lat] coordinate pairs (see map_utils.wkt_to_lonlat_path).
"""

from __future__ import annotations

import base64
import os
import re
from datetime import date as date_type
from pathlib import Path
from typing import Any, List, Optional

import httpx
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
from shiny import App, reactive, render, ui

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

from api_client import fetch_segments, fetch_traffic_window
from llm_cloud import OllamaCloudError, query_llm
from map_utils import (
    BAR_HARBOR_CENTER_LAT,
    BAR_HARBOR_CENTER_LON,
    build_map_data,
    vc_severity_label,
    vc_to_color,
)

# Ollama Cloud: no local server. API key and model come from llm_cloud (OLLAMA_API_KEY, OLLAMA_MODEL).

DRIVEABLE = {
    "motorway", "trunk", "primary", "secondary", "tertiary",
    "residential", "unclassified", "service", "living_street",
    "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link",
}

DEFAULT_API_BASE = "https://connect.systems-apps.com/content/ac3cdefe-f3cc-43b8-86fd-7f212e7263d2"

ZOOM_LOCATIONS = {
    "Bar Harbor (overview)": (BAR_HARBOR_CENTER_LAT, BAR_HARBOR_CENTER_LON, 12),
    "Downtown Bar Harbor": (44.392, -68.204, 15),
}


def sanitize_report_text(raw: str) -> str:
    """
    Post-process LLM output so it reads as plain report text, not markdown.
    Strips markdown artifacts, normalizes punctuation, and collapses excess whitespace.
    """
    if not raw or not isinstance(raw, str):
        return (raw or "").strip()
    s = raw
    # Remove markdown bold/italic: **text** and __text__
    s = re.sub(r"\*\*([^*]*)\*\*", r"\1", s)
    s = re.sub(r"__([^_]*)__", r"\1", s)
    # Remove leading markdown headers (# ## ###) and optional space after
    s = re.sub(r"^#{1,6}\s*", "", s, flags=re.MULTILINE)
    # Remove backticks (inline code)
    s = s.replace("`", "")
    # Normalize common unicode punctuation to ASCII-style where it looks awkward
    s = s.replace("\u2013", "-").replace("\u2014", "-")  # en dash, em dash -> hyphen
    s = s.replace("\u2018", "'").replace("\u2019", "'")   # smart single quotes
    s = s.replace("\u201c", '"').replace("\u201d", '"')   # smart double quotes
    # Collapse 3+ newlines to at most 2 (one blank line)
    s = re.sub(r"\n{3,}", "\n\n", s)
    # Trim each line and drop leading/trailing blank lines
    lines = [line.rstrip() for line in s.splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines).strip()


def make_deck(map_df: pd.DataFrame, view_lat: float, view_lon: float, view_zoom: float, daily_mode: bool = False) -> pdk.Deck:
    """PyDeck PathLayer expects get_path = list of [lon, lat] pairs per row (see map_utils)."""
    layer = pdk.Layer(
        "PathLayer",
        data=map_df,
        get_path="path",
        get_color="color",
        width_scale=12,
        width_min_pixels=1.5,
        get_width=2.5,
        pickable=True,
    )
    view = pdk.ViewState(latitude=view_lat, longitude=view_lon, zoom=view_zoom, pitch=0, bearing=0)
    if daily_mode and "tooltip_peak_vc" in map_df.columns and "tooltip_peak_hour" in map_df.columns:
        tooltip = {"html": "<b>{street_name}</b><br/>Peak V/C: {tooltip_peak_vc} at {tooltip_peak_hour}<br/>Avg speed: {tooltip_speed} km/h<br/>Flow: {tooltip_flow} vph", "style": {"backgroundColor": "white", "border": "1px solid #e2e8f0"}}
    else:
        tooltip = {"html": "<b>{street_name}</b><br/>Speed: {tooltip_speed} km/h<br/>Flow: {tooltip_flow} vph<br/>V/C: {tooltip_vc}", "style": {"backgroundColor": "white", "border": "1px solid #e2e8f0"}}
    if "tooltip_speed" not in map_df.columns:
        tooltip = {"html": "<b>{street_name}</b><br/>Segment: {segment_id}", "style": {"backgroundColor": "white"}}
    return pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip, map_style="light", map_provider="carto")


def _map_error_html(height: int, message: str = "Map could not be generated.") -> str:
    return f'<div style="padding:2rem;text-align:center;color:#64748b;height:{height}px;display:flex;align-items:center;justify-content:center;">{message}</div>'


def deck_to_embed_html(deck: pdk.Deck, height: int = 520, use_iframe: bool = True) -> str:
    try:
        raw = deck.to_html(as_string=True, iframe_height=height, iframe_width="100%")
    except Exception:
        return _map_error_html(height, "Map could not be generated.")
    if raw is None or not isinstance(raw, str) or not raw.strip():
        return _map_error_html(height, "Map could not be generated.")
    if use_iframe:
        try:
            b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        except Exception:
            return _map_error_html(height, "Map could not be generated.")
        return f'<iframe src="data:text/html;base64,{b64}" width="100%" height="{height}" style="border:none;border-radius:12px;"></iframe>'
    return f'<div style="width:100%;height:{height}px;">{raw}</div>'


def _plotly_to_iframe_html(fig: go.Figure, height: int, div_id: str, include_plotlyjs: str = "cdn") -> str:
    try:
        raw = fig.to_html(include_plotlyjs=include_plotlyjs, config={"displayModeBar": True}, div_id=div_id)
    except Exception:
        return f'<div style="height:{height}px;display:flex;align-items:center;justify-content:center;color:#64748b;">Chart could not be generated.</div>'
    if not raw or not isinstance(raw, str):
        return f'<div style="height:{height}px;display:flex;align-items:center;justify-content:center;color:#64748b;">Chart could not be generated.</div>'
    try:
        b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
    except Exception:
        return f'<div style="height:{height}px;display:flex;align-items:center;justify-content:center;color:#64748b;">Chart could not be generated.</div>'
    return f'<iframe src="data:text/html;base64,{b64}" width="100%" height="{height}" style="border:none;border-radius:8px;"></iframe>'


# Inline favicon so browser doesn't request /favicon.ico (avoids 404 in terminal)
FAVICON_SVG = (
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E"
    "%3Cpath fill='%231e293b' d='M16 2L4 10v12h8v-8h8v8h8V10L16 2z'/%3E%3C/svg%3E"
)

# --- UI ---
app_ui = ui.page_fluid(
    ui.tags.head(
        ui.tags.link(rel="icon", href=FAVICON_SVG, type="image/svg+xml"),
        ui.tags.style("""
        .dashboard-card { background: white; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); padding: 1.25rem; margin-bottom: 1rem; }
        .metric-card { background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); border-radius: 12px; padding: 1.25rem; text-align: center; }
        .metric-value { font-size: 2rem; font-weight: 700; color: #0f172a; }
        .metric-label { font-size: 0.8rem; color: #64748b; text-transform: uppercase; margin-top: 0.25rem; }
        .viz-debug { font-family: monospace; font-size: 0.75rem; background: #fefce8; border: 1px solid #eab308; padding: 0.75rem; border-radius: 8px; margin-top: 0.5rem; }
        html, body { overflow-x: hidden; overflow-y: auto; height: auto; min-height: 100%; }
        .main-content { overflow: visible; }
        .bslib-navbar { background: #1e293b; color: #f1f5f9; padding: 0.75rem 1.5rem; border-radius: 0; }
        .bslib-navbar .navbar-brand { font-size: 1.25rem; font-weight: 600; color: #f1f5f9; }
        .data-resolution-wrap { border-radius: 8px; border: 1px solid #cbd5e1; background: #f1f5f9; padding: 2px; display: inline-flex; flex-wrap: nowrap; }
        .data-resolution-wrap .form-check { margin: 0; flex: 1; }
        .data-resolution-wrap .form-check-inline { margin-right: 0; }
        .data-resolution-wrap .form-check-input { display: none; }
        .data-resolution-wrap .form-check-label { margin: 0; cursor: pointer; display: block; text-align: center; padding: 0.4rem 1rem; font-weight: 500; color: #64748b; border-radius: 6px; }
        .data-resolution-wrap .form-check-input:checked + .form-check-label { background: #1e293b; color: #f1f5f9; }
    """)),
    ui.tags.nav(
        {"class": "bslib-navbar"},
        ui.tags.div(
            {"class": "container-fluid d-flex align-items-center"},
            ui.tags.span({"class": "navbar-brand me-4"}, ui.HTML('<span style="margin-right:0.5rem;">🗺️</span>'), "Bar Harbor Congestion Intelligence Dashboard"),
            ui.tags.span({"class": "ms-auto text-muted", "style": "font-size:0.9rem;"}, ui.HTML('<span title="Traffic data from Bar Harbor Traffic Report API">📡 API</span>')),
        ),
    ),
    ui.layout_sidebar(
        ui.sidebar(
            ui.tags.h6("Data Resolution"),
            ui.tags.div(
                {"class": "data-resolution-wrap", "style": "margin-bottom: 1rem;"},
                ui.input_radio_buttons(
                    "data_mode",
                    None,
                    choices={"hourly": "Hourly", "daily": "Daily"},
                    selected="hourly",
                    inline=True,
                ),
            ),
            ui.tags.h6("Filters", {"class": "mt-3"}),
            ui.input_text("api_base", "API base URL", value=DEFAULT_API_BASE),
            ui.input_date("date", "Date", value=date_type(2025, 3, 4), min=date_type(2025, 3, 3), max=date_type(2025, 3, 9)),
            ui.panel_conditional("input.data_mode === 'hourly'", ui.input_numeric("hour", "Hour", value=18, min=0, max=23)),
            ui.input_checkbox("driveable_only", "Driveable roads only", value=True),
            ui.input_action_button("load_btn", "Load traffic", class_="btn-primary w-100"),
            ui.div({"class": "mt-2 p-2", "style": "min-height: 3rem; border: 1px solid #e2e8f0; border-radius: 8px; background: #fff;"},
                ui.tags.h6("Status", {"class": "mb-1", "style": "font-size: 0.75rem; color: #64748b; text-transform: uppercase;"}),
                ui.output_ui("loading_status_ui"),
            ),
            ui.div({"class": "mt-2 p-2", "style": "font-size: 0.75rem; color: #64748b; border-radius: 8px; background: #f8fafc; border: 1px solid #e2e8f0;"},
                ui.tags.strong("V/C"),
                " = volume-to-capacity ratio (flow ÷ capacity). Values > 0.8 indicate congestion. ",
                ui.tags.strong("vph"),
                " = vehicles per hour.",
            ),
            title="Filters",
            width=280,
        ),
        ui.div(
            {"class": "main-content", "style": "padding: 1.5rem;"},
            ui.row(
                ui.column(3, ui.div({"class": "metric-card"}, ui.div({"class": "metric-value"}, ui.output_text("metric_avg_speed")), ui.div({"class": "metric-label"}, ui.output_text("metric_avg_speed_label")), ui.HTML('<div style="margin-top:0.5rem;font-size:1.5rem;" title="Average speed">🚗</div>'))),
                ui.column(3, ui.div({"class": "metric-card"}, ui.div({"class": "metric-value"}, ui.output_text("metric_flow")), ui.div({"class": "metric-label"}, ui.output_text("metric_flow_label")), ui.HTML('<div style="margin-top:0.5rem;font-size:1.5rem;" title="Flow">📈</div>'))),
                ui.column(3, ui.div({"class": "metric-card"}, ui.div({"class": "metric-value"}, ui.output_text("metric_third")), ui.div({"class": "metric-label"}, ui.output_text("metric_third_label")), ui.HTML('<div style="margin-top:0.5rem;font-size:1.5rem;" title="Congestion">⚠️</div>'))),
                ui.column(3, ui.div({"class": "metric-card"}, ui.div({"class": "metric-value"}, ui.output_text("metric_fourth")), ui.div({"class": "metric-label"}, ui.output_text("metric_fourth_label")), ui.HTML('<div style="margin-top:0.5rem;font-size:1.5rem;" title="Data">📥</div>'))),
            ),
            ui.tags.hr(),
            ui.row(
                ui.column(12, ui.div({"class": "dashboard-card"}, ui.tags.h5("Congestion map"), ui.output_ui("dataset_badge_ui"), ui.output_ui("map_ui")),
                ),
            ),
            ui.row(
                ui.column(4, ui.div({"class": "dashboard-card"}, ui.tags.h5("Overall congestion"), ui.output_ui("gauge_ui"))),
                ui.panel_conditional("input.data_mode === 'daily'", ui.column(8, ui.div({"class": "dashboard-card"}, ui.tags.h5("Time of day profile"), ui.output_ui("plotly_ui")))),
            ),
            ui.row(
                ui.column(12, ui.div({"class": "dashboard-card"}, ui.tags.h5("Most Congested Roads"), ui.output_ui("table_ui"))),
            ),
            ui.row(
                ui.column(12, ui.div({"class": "dashboard-card"}, ui.tags.h5("AI Analysis"), ui.input_action_button("ai_analysis_btn", "Generate AI summary", class_="btn-primary mb-2"), ui.output_ui("ai_summary_ui"))),
            ),
        ),
    ),
    title="Bar Harbor Congestion Intelligence Dashboard",
)


def server(input, output, session):
    segments_df = reactive.Value(None)
    observations_df = reactive.Value(None)
    window_stats_df = reactive.Value(None)
    loading = reactive.Value(False)
    loading_message = reactive.Value("")
    api_error = reactive.Value(None)

    # Visualization debug: values set inside map_ui and gauge_ui (panel removed; kept for internal use)
    viz_debug_map_len = reactive.Value(None)
    viz_debug_sample_path = reactive.Value("")
    viz_debug_gauge_value = reactive.Value(None)
    viz_debug_color_sample = reactive.Value("")

    ai_summary_text = reactive.Value("")
    ai_loading = reactive.Value(False)

    def _do_load():
        """Fetch segments and observations from API using current input values. Used by both button and reactive effect."""
        loading.set(True)
        loading_message.set("Fetching road segments…")
        api_base = (input.api_base() or "").strip() or None
        if not api_base:
            api_error.set("Please enter API base URL")
            loading.set(False)
            loading_message.set("")
            return
        api_error.set(None)
        date_val = input.date()
        if date_val is not None and hasattr(date_val, "isoformat"):
            date_str = date_val.isoformat()
        elif date_val is not None:
            date_str = str(date_val).strip()[:10]
        else:
            date_str = "2025-03-04"
        if len(date_str) != 10 or date_str[4] != "-" or date_str[7] != "-":
            date_str = "2025-03-04"
        mode = (input.data_mode() or "hourly").strip().lower()
        if mode == "daily":
            start_hour, end_hour = 0, 23
        else:
            try:
                h = input.hour()
                start_hour = int(h) if h is not None else 18
                end_hour = start_hour + 1
            except (TypeError, ValueError):
                start_hour, end_hour = 18, 19
        # Debug: API query parameters (check terminal to confirm date changes when you pick a new day)
        print(f"[Congestion] API query: date={date_str!r}, start_hour={start_hour}, end_hour={end_hour}, data_mode={mode}")
        with ui.Progress(min=0, max=1, session=session) as p:
            p.set(0, message="Loading traffic data…", detail="Step 1 of 2: Fetching road segments")
            seg, err, _, _ = fetch_segments(api_base)
            if err:
                api_error.set(err)
                segments_df.set(None)
                observations_df.set(None)
                window_stats_df.set(None)
                loading.set(False)
                loading_message.set("")
                return
            if input.driveable_only():
                seg = seg[seg["road_class"].astype(str).str.lower().isin(DRIVEABLE)]
            segments_df.set(seg)
            if mode == "daily":
                # Daily: fetch one window per hour (0-1, 1-2, ..., 22-23) so we can compute peak congestion hour
                p.set(0.5, message="Loading traffic data…", detail=f"Step 2 of 2: Fetching hourly windows for {date_str}…")
                loading_message.set(f"Fetching hourly windows for {date_str}…")
                hourly_dfs = []
                num_hours = 23
                for h in range(num_hours):
                    loading_message.set(f"Fetching hour {h + 1}/{num_hours}…")
                    wh, err_h, _, _ = fetch_traffic_window(api_base, date=date_str, start_hour=h, end_hour=h + 1)
                    if err_h or wh is None or wh.empty:
                        api_error.set(err_h or "No data for one or more hours")
                        observations_df.set(None)
                        window_stats_df.set(None)
                        loading.set(False)
                        loading_message.set("")
                        return
                    wh = wh.copy()
                    wh["hour"] = h
                    hourly_dfs.append(wh)
                combined = pd.concat(hourly_dfs, ignore_index=True)
                daily_agg = combined.groupby("segment_id", as_index=False).agg(
                    mean_flow_vph=("mean_flow_vph", "mean"),
                    mean_speed_kmh=("mean_speed_kmh", "mean"),
                    mean_travel_time_sec=("mean_travel_time_sec", "mean"),
                    vc_ratio=("vc_ratio", "mean"),
                )
                window_stats_df.set(daily_agg)
                obs_daily = combined[["segment_id", "mean_flow_vph", "mean_speed_kmh", "vc_ratio", "hour"]].copy()
                obs_daily = obs_daily.rename(columns={"mean_flow_vph": "flow_vph", "mean_speed_kmh": "speed_kmh"})
                obs_daily["timestamp"] = pd.Timestamp(date_str) + pd.to_timedelta(obs_daily["hour"], unit="h")
                observations_df.set(obs_daily.drop(columns=["hour"]))
                print(f"[DEBUG] Loaded daily for date={date_str!r}: {len(combined)} rows (23 hours), {len(daily_agg)} segments")
            else:
                p.set(0.5, message="Loading traffic data…", detail="Step 2 of 2: Fetching traffic window")
                loading_message.set("Fetching traffic window…")
                window_df, err2, _, _ = fetch_traffic_window(api_base, date=date_str, start_hour=start_hour, end_hour=end_hour)
                if err2:
                    api_error.set(err2)
                    observations_df.set(None)
                    window_stats_df.set(None)
                else:
                    window_stats_df.set(window_df)
                    ts = pd.Timestamp(date_str) + pd.Timedelta(hours=start_hour)
                    synthetic = window_df[["segment_id", "mean_flow_vph", "mean_speed_kmh", "vc_ratio"]].copy()
                    synthetic = synthetic.rename(columns={"mean_flow_vph": "flow_vph", "mean_speed_kmh": "speed_kmh"})
                    synthetic["timestamp"] = ts
                    observations_df.set(synthetic)
                    print(f"[DEBUG] Loaded window: {len(window_df)} segments for {date_str}, hours {start_hour}-{end_hour}")
            p.set(1, message="Done", detail="Data loaded")
        loading_message.set("Done")
        loading.set(False)

    @reactive.Effect
    def _auto_load_on_input_change():
        """Re-fetch observations whenever date, data_mode, or hour changes so map and metrics update."""
        input.date()
        input.data_mode()
        input.hour()
        _do_load()

    @reactive.Effect
    @reactive.event(input.load_btn)
    def _load_data():
        _do_load()

    @reactive.Calc
    def segments():
        return segments_df.get()

    @reactive.Calc
    def observations():
        """Return observations filtered by selected date/hour/mode. Invalidates when inputs or observations_df change."""
        date_val = input.date()
        mode = (input.data_mode() or "hourly").strip().lower()
        hour_val = input.hour()
        obs = observations_df.get()
        if obs is None or not isinstance(obs, pd.DataFrame) or obs.empty:
            return obs
        if "timestamp" not in obs.columns:
            return obs
        ts = pd.to_datetime(obs["timestamp"], utc=False)
        if date_val is None:
            return obs
        target_date = (
            date_val if hasattr(date_val, "year") else pd.Timestamp(str(date_val)[:10]).date()
        )
        if mode == "daily":
            mask = ts.dt.date == target_date
            sh, eh = 0, 23
        else:
            try:
                target_hour = int(hour_val) if hour_val is not None else 18
            except (TypeError, ValueError):
                target_hour = 18
            mask = (ts.dt.date == target_date) & (ts.dt.hour == target_hour)
            sh = eh = target_hour
        filtered = obs.loc[mask].copy()
        # Debug: confirm slice
        print(f"[DEBUG] Loaded {len(filtered)} rows for {target_date}, hours {sh}-{eh}")
        if not filtered.empty and "timestamp" in filtered.columns:
            hrs = pd.to_datetime(filtered["timestamp"], utc=False).dt.hour.unique().tolist()
            print(f"[DEBUG] timestamp hours in data: {sorted(hrs)}")
        return filtered

    @reactive.Effect
    def _debug_seg_stats():
        """Temporary debug: print mean vc_ratio after aggregation when seg_stats updates."""
        stats = seg_stats()
        if stats is not None and isinstance(stats, pd.DataFrame) and not stats.empty and "vc_ratio" in stats.columns:
            mean_vc = stats["vc_ratio"].mean()
            print(f"[Congestion] seg_stats updated: mean vc_ratio = {mean_vc:.4f} (n_segments={len(stats)})")

    @reactive.Calc
    def peak_hour_for_map():
        """Network-wide peak congestion hour (0-23) for daily mode; None in hourly mode. Used so the daily map shows the worst hour."""
        mode = (input.data_mode() or "hourly").strip().lower()
        if mode != "daily":
            return None
        obs = observations()
        seg = segments()
        if obs is None or not isinstance(obs, pd.DataFrame) or obs.empty or "timestamp" not in obs.columns or "flow_vph" not in obs.columns:
            return None
        ts = pd.to_datetime(obs["timestamp"], utc=False)
        obs = obs.copy()
        obs["_hour"] = ts.dt.hour
        agg = obs.groupby("_hour")["flow_vph"].mean()
        if agg.empty or len(agg) < 2:
            return int(agg.idxmax()) if not agg.empty else None
        cap_avg = seg["capacity_vph"].mean() if seg is not None and isinstance(seg, pd.DataFrame) and not seg.empty and "capacity_vph" in seg.columns else 0
        if not cap_avg or cap_avg <= 0:
            return int(agg.idxmax())
        vc_by_hour = agg / cap_avg
        return int(vc_by_hour.idxmax())

    @reactive.Calc
    def seg_stats():
        # Use aggregated window from API when available (no client-side groupby)
        window = window_stats_df.get()
        if window is not None and isinstance(window, pd.DataFrame) and not window.empty:
            return window.copy()
        obs = observations()
        seg = segments()
        if obs is None or seg is None or not isinstance(obs, pd.DataFrame) or not isinstance(seg, pd.DataFrame) or obs.empty or seg.empty:
            return None
        mode = (input.data_mode() or "hourly").strip().lower()
        agg_cols = {"mean_flow_vph": ("flow_vph", "mean")}
        if "speed_kmh" in obs.columns:
            agg_cols["mean_speed_kmh"] = ("speed_kmh", "mean")
        agg = obs.groupby("segment_id").agg(**agg_cols).reset_index()
        if "mean_speed_kmh" not in agg.columns:
            agg["mean_speed_kmh"] = np.nan
        cap = seg.set_index("segment_id")["capacity_vph"]
        capacity = agg["segment_id"].map(cap)
        agg["vc_ratio"] = np.where(capacity > 0, agg["mean_flow_vph"] / capacity, np.nan)
        agg["vc_ratio"] = agg["vc_ratio"].round(2)
        agg["mean_speed_kmh"] = agg["mean_speed_kmh"].round(1)
        agg["mean_flow_vph"] = agg["mean_flow_vph"].round(0)
        # Daily mode: add peak v/c and hour of peak per segment (for map coloring and tooltip)
        if mode == "daily" and "timestamp" in obs.columns:
            obs_cap = obs["segment_id"].map(cap)
            obs_vc = np.where(obs_cap > 0, obs["flow_vph"].astype(float) / obs_cap, np.nan)
            obs = obs.copy()
            obs["_vc"] = obs_vc
            peak_idx = obs.groupby("segment_id")["_vc"].idxmax()
            peak_idx = peak_idx.dropna()
            if peak_idx.empty:
                return agg
            peak_rows = obs.loc[peak_idx, ["segment_id", "_vc"]].copy()
            peak_rows["peak_vc"] = peak_rows["_vc"].round(2)
            ts = pd.to_datetime(obs["timestamp"], utc=False)
            peak_rows["peak_hour"] = ts.loc[peak_idx].values
            peak_rows["peak_hour"] = pd.to_datetime(peak_rows["peak_hour"], utc=False).dt.hour
            agg = agg.merge(peak_rows[["segment_id", "peak_vc", "peak_hour"]], on="segment_id", how="left")
        return agg

    @reactive.Calc
    def map_df():
        seg = segments()
        if seg is None or not isinstance(seg, pd.DataFrame) or seg.empty:
            return None
        mode = (input.data_mode() or "hourly").strip().lower()
        used_peak_hour = False  # True when daily map is built from peak-hour slice
        # Daily mode: map shows the network-wide worst hour (same as KPI "Peak congestion hour")
        if mode == "daily":
            peak_h = peak_hour_for_map()
            obs = observations()
            if peak_h is not None and obs is not None and isinstance(obs, pd.DataFrame) and not obs.empty and "timestamp" in obs.columns:
                ts = pd.to_datetime(obs["timestamp"], utc=False)
                obs_peak = obs.loc[ts.dt.hour == peak_h]
                if not obs_peak.empty:
                    agg_cols = {"mean_flow_vph": ("flow_vph", "mean")}
                    if "speed_kmh" in obs_peak.columns:
                        agg_cols["mean_speed_kmh"] = ("speed_kmh", "mean")
                    agg = obs_peak.groupby("segment_id").agg(**agg_cols).reset_index()
                    if "mean_speed_kmh" not in agg.columns:
                        agg["mean_speed_kmh"] = np.nan
                    cap = seg.set_index("segment_id")["capacity_vph"]
                    cap_mapped = agg["segment_id"].map(cap)
                    agg["vc_ratio"] = np.where(cap_mapped > 0, agg["mean_flow_vph"] / cap_mapped, np.nan)
                    agg["vc_ratio"] = agg["vc_ratio"].round(2)
                    agg["mean_speed_kmh"] = agg["mean_speed_kmh"].round(1)
                    agg["mean_flow_vph"] = agg["mean_flow_vph"].round(0)
                    stats = agg
                    used_peak_hour = True
                else:
                    stats = seg_stats()
            else:
                stats = seg_stats()
        else:
            stats = seg_stats()
        df = build_map_data(seg, stats)
        if df is None or df.empty:
            return df
        for col, src, fmt in (
            ("tooltip_vc", "vc_ratio", lambda x: f"{x:.2f}" if pd.notna(x) else "—"),
            ("tooltip_speed", "mean_speed_kmh", lambda x: f"{x:.1f}" if pd.notna(x) else "—"),
            ("tooltip_flow", "mean_flow_vph", lambda x: f"{x:.0f}" if pd.notna(x) else "—"),
        ):
            if src in df.columns and col not in df.columns:
                df[col] = df[src].apply(fmt)
        # Daily map: when map uses peak-hour data, tooltip shows V/C, speed, flow at that hour
        if mode == "daily" and used_peak_hour:
            ph = peak_hour_for_map()
            if ph is not None:
                df["tooltip_peak_vc"] = df["vc_ratio"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
                df["tooltip_peak_hour"] = f"{int(ph)}:00"
        return df

    @reactive.Calc
    def view_state():
        return BAR_HARBOR_CENTER_LAT, BAR_HARBOR_CENTER_LON, 13.5

    @render.text
    def metric_avg_speed_label():
        return "Avg speed (daily mean)" if (input.data_mode() or "").strip().lower() == "daily" else "Avg speed (km/h)"

    @render.text
    def metric_flow_label():
        return "Avg flow (daily mean), vph" if (input.data_mode() or "").strip().lower() == "daily" else "Mean Flow (vph)"

    @render.text
    def metric_third_label():
        return "Peak congestion hour" if (input.data_mode() or "").strip().lower() == "daily" else "Congested (v/c > 0.8)"

    @render.text
    def metric_fourth_label():
        return "Worst Street" if (input.data_mode() or "").strip().lower() == "daily" else "Observations"

    @render.text
    def metric_avg_speed():
        stats = seg_stats()
        if stats is None or not isinstance(stats, pd.DataFrame) or stats.empty or "mean_speed_kmh" not in stats.columns:
            obs = observations()
            if obs is None or not isinstance(obs, pd.DataFrame) or obs.empty or "speed_kmh" not in obs.columns:
                return "—"
            s = obs["speed_kmh"].replace(0, np.nan).mean()
            return f"{s:.1f}" if pd.notna(s) else "—"
        s = stats["mean_speed_kmh"].replace(0, np.nan).mean()
        return f"{s:.1f}" if pd.notna(s) else "—"

    @render.text
    def metric_flow():
        stats = seg_stats()
        if stats is not None and isinstance(stats, pd.DataFrame) and not stats.empty and "mean_flow_vph" in stats.columns:
            m = stats["mean_flow_vph"].mean()
            return f"{m:.0f}" if pd.notna(m) else "—"
        obs = observations()
        if obs is None or not isinstance(obs, pd.DataFrame) or obs.empty or "flow_vph" not in obs.columns:
            return "—"
        m = obs["flow_vph"].mean()
        return f"{m:.0f}" if pd.notna(m) else "—"

    @render.text
    def metric_third():
        mode = (input.data_mode() or "hourly").strip().lower()
        if mode == "hourly":
            stats = seg_stats()
            if stats is None or not isinstance(stats, pd.DataFrame) or stats.empty:
                return "0"
            n = (stats["vc_ratio"] >= 0.8).sum()
            return str(int(n))
        try:
            obs = observations()
            if obs is None or not isinstance(obs, pd.DataFrame) or obs.empty:
                return "—"
            if "timestamp" not in obs.columns or "flow_vph" not in obs.columns:
                return "—"
            obs = obs.copy()
            obs["hour"] = pd.to_datetime(obs["timestamp"], utc=False).dt.hour
            agg = obs.groupby("hour")["flow_vph"].mean()
            if agg.empty or len(agg) < 2:
                return "—"
            seg = segments()
            cap_avg = seg["capacity_vph"].mean() if seg is not None and isinstance(seg, pd.DataFrame) and not seg.empty and "capacity_vph" in seg.columns else 0
            if not cap_avg or cap_avg <= 0:
                peak_h = int(agg.idxmax())
            else:
                vc_by_hour = agg / cap_avg
                peak_h = int(vc_by_hour.idxmax())
            return f"{peak_h}:00"
        except Exception:
            return "—"

    @render.text
    def metric_fourth():
        mode = (input.data_mode() or "hourly").strip().lower()
        if mode == "hourly":
            obs = observations()
            if obs is None or not isinstance(obs, pd.DataFrame):
                return "0"
            return f"{len(obs):,}"
        stats = seg_stats()
        seg = segments()
        if stats is None or seg is None or not isinstance(stats, pd.DataFrame) or not isinstance(seg, pd.DataFrame) or stats.empty:
            return "—"
        merge = stats.merge(seg[["segment_id", "street_name"]], on="segment_id", how="left")
        merge["street_name"] = merge["street_name"].fillna("").astype(str).replace("", "(unnamed)")
        vc_col = "peak_vc" if "peak_vc" in merge.columns else "vc_ratio"
        by_street = merge.groupby("street_name")[vc_col].max().reset_index()
        by_street = by_street.sort_values(vc_col, ascending=False)
        if by_street.empty:
            return "—"
        worst_name = str(by_street.iloc[0]["street_name"]).strip()
        return worst_name if worst_name else "—"

    @render.ui
    def dataset_badge_ui():
        mode = (input.data_mode() or "hourly").strip().lower()
        if mode == "daily":
            text = "Dataset: Daily Aggregated Traffic"
        else:
            text = "Dataset: Hourly Snapshot"
        badge = ui.tags.span(
            text,
            style="display: inline-block; margin-bottom: 0.75rem; padding: 0.35rem 0.75rem; font-size: 0.8rem; font-weight: 600; color: #1e293b; background: #e2e8f0; border-radius: 6px;",
        )
        if mode == "daily":
            ph = peak_hour_for_map()
            caption = f"Daily map: showing congestion at peak hour ({int(ph)}:00)" if ph is not None else "Daily map: showing congestion at peak hour"
            return ui.div(
                badge,
                ui.tags.p(caption, style="font-size: 0.75rem; color: #64748b; margin: 0 0 0.75rem 0;"),
            )
        return badge

    # Map: @render.ui must return the UI object. Connected via ui.output_ui("map_ui").
    # PyDeck PathLayer expects path = list of [lon, lat] pairs (see map_utils.wkt_to_lonlat_path).
    @render.ui
    def map_ui():
        if loading():
            viz_debug_map_len.set(None)
            viz_debug_sample_path.set("(loading)")
            viz_debug_color_sample.set("")
            return ui.HTML('<div class="skeleton" style="height:520px;"></div>')
        df = map_df()
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            viz_debug_map_len.set(0)
            viz_debug_sample_path.set("(no map_df)")
            viz_debug_color_sample.set("(no map_df)")
            return ui.div("Load segments and click Load traffic to see the map.", style="padding: 2rem; text-align: center; color: #64748b;")
        df = df.dropna(subset=["path"]).copy()
        n_before = len(df)
        for col in ("tooltip_vc", "tooltip_speed", "tooltip_flow", "tooltip_peak_vc", "tooltip_peak_hour"):
            if col in df.columns:
                df[col] = df[col].fillna("—").astype(str)
        if "color" in df.columns:
            df["color"] = df["color"].apply(lambda c: [int(x) for x in c] if isinstance(c, (list, tuple)) and len(c) == 3 else [180, 180, 180])
        # Debug: first 10 rows vc_ratio and color (must be list of 3 ints 0-255 for PyDeck)
        if "vc_ratio" in df.columns and "color" in df.columns:
            sample_lines = []
            for i in range(min(10, len(df))):
                vc = df["vc_ratio"].iloc[i]
                cl = df["color"].iloc[i]
                vc_str = f"{vc:.4f}" if pd.notna(vc) else "NaN"
                cl_ok = isinstance(cl, (list, tuple)) and len(cl) == 3 and all(isinstance(x, (int, float)) and 0 <= x <= 255 for x in cl)
                sample_lines.append(f"  row {i}: vc_ratio={vc_str}, color={list(cl)}, valid_3int={cl_ok}")
            viz_debug_color_sample.set("\n".join(sample_lines))
        else:
            viz_debug_color_sample.set("(vc_ratio or color column missing)")
        for col in ("street_name", "segment_id", "tooltip_vc", "tooltip_speed", "tooltip_flow", "tooltip_peak_vc", "tooltip_peak_hour"):
            if col in df.columns:
                df[col] = df[col].fillna("—").astype(str).replace("nan", "—")
        if "segment_id" in df.columns:
            df["segment_id"] = df["segment_id"].astype(str)

        def path_to_python(p):
            if not isinstance(p, (list, tuple)) or len(p) < 2:
                return None
            try:
                return [[float(coord[0]), float(coord[1])] for coord in p]
            except (IndexError, TypeError, ValueError):
                return None

        df["path"] = df["path"].apply(path_to_python)
        df = df.dropna(subset=["path"]).copy()
        n_for_layer = len(df)

        # Visualization debug: store for display in viz_debug_ui
        viz_debug_map_len.set(n_for_layer)
        sample_paths: List[str] = []
        for i, path in enumerate(df["path"].head(3)):
            sample_paths.append(f"  row {i}: {path[:4]}{'...' if len(path) > 4 else ''}")
        viz_debug_sample_path.set("\n".join(sample_paths) if sample_paths else "(none)")

        if df.empty:
            return ui.div("No valid segment geometry for the map.", style="padding: 2rem; text-align: center; color: #64748b;")
        lat, lon, zoom = view_state()
        daily_mode = (input.data_mode() or "").strip().lower() == "daily"
        try:
            deck = make_deck(df, lat, lon, zoom, daily_mode=daily_mode)
            # Embed map in iframe so PyDeck's full HTML document doesn't break main page scrolling
            embed = deck_to_embed_html(deck, height=520, use_iframe=True)
            out = ui.HTML(embed)
        except Exception as e:
            out = ui.HTML(_map_error_html(520, f"Map could not be generated: {str(e)[:80]}"))
        return out

    # Gauge: @render.ui must return the UI object. Connected via ui.output_ui("gauge_ui").
    @render.ui
    def gauge_ui():
        if loading():
            viz_debug_gauge_value.set(None)
            return ui.HTML('<div class="skeleton" style="height:220px;"></div>')
        stats = seg_stats()
        if stats is None or not isinstance(stats, pd.DataFrame) or stats.empty:
            viz_debug_gauge_value.set(None)
            return ui.div("Load traffic to see overall congestion.", style="color: #64748b; padding: 1rem;")
        avg_vc = stats["vc_ratio"].mean()
        if pd.isna(avg_vc):
            avg_vc = 0.0
        gauge_value = round(avg_vc, 2)
        viz_debug_gauge_value.set(gauge_value)
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=gauge_value,
            number={"suffix": " avg V/C"},
            gauge={"axis": {"range": [0, 1.2]}, "bar": {"color": "darkblue"}, "steps": [
                {"range": [0, 0.5], "color": "rgba(34, 139, 34, 0.4)"},
                {"range": [0.5, 0.7], "color": "rgba(255, 215, 0, 0.4)"},
                {"range": [0.7, 0.9], "color": "rgba(255, 140, 0, 0.4)"},
                {"range": [0.9, 1.2], "color": "rgba(220, 20, 20, 0.4)"},
            ], "threshold": {"line": {"color": "red", "width": 4}, "value": 1.0}},
            title={"text": "Average V/C"},
        ))
        fig.update_layout(height=220, margin=dict(l=20, r=20, t=50, b=20), paper_bgcolor="rgba(0,0,0,0)")
        return ui.HTML(_plotly_to_iframe_html(fig, height=220, div_id="gauge-plot", include_plotlyjs="cdn"))

    @render.ui
    def plotly_ui():
        if loading():
            return ui.HTML('<div class="skeleton" style="height:280px;"></div>')
        obs = observations()
        seg = segments()
        if obs is None or seg is None or not isinstance(obs, pd.DataFrame) or obs.empty:
            return ui.div("Load traffic to see time-of-day profile.", style="color: #64748b; padding: 1rem;")
        obs = obs.copy()
        try:
            obs["hour"] = pd.to_datetime(obs["timestamp"], utc=False).dt.hour
        except Exception:
            return ui.div("Load traffic to see time-of-day profile.", style="color: #64748b; padding: 1rem;")
        agg_dict = {"mean_flow": ("flow_vph", "mean")}
        if "speed_kmh" in obs.columns:
            agg_dict["mean_speed"] = ("speed_kmh", "mean")
        by_hour = obs.groupby("hour").agg(**agg_dict).reset_index()
        if by_hour.empty:
            return ui.div("No hourly data.", style="color: #64748b; padding: 1rem;")
        cap_avg = seg["capacity_vph"].mean() if isinstance(seg, pd.DataFrame) and not seg.empty and "capacity_vph" in seg.columns else 0
        by_hour["mean_vc"] = (by_hour["mean_flow"] / cap_avg) if cap_avg and cap_avg > 0 else 0.0
        peak_hour = float(by_hour.loc[by_hour["mean_vc"].idxmax(), "hour"]) if len(by_hour) > 0 else None
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=by_hour["hour"].astype(float), y=by_hour["mean_vc"], mode="lines+markers", name="Mean V/C", line=dict(color="#0ea5e9", width=2)))
        if peak_hour is not None:
            fig.add_vline(x=peak_hour, line_dash="dash", line_color="orange", annotation_text="Peak")
        fig.update_layout(xaxis_title="Hour of day", yaxis_title="Mean V/C", height=280, margin=dict(l=50, r=20, t=20, b=50), showlegend=False, paper_bgcolor="rgba(0,0,0,0)")
        return ui.HTML(_plotly_to_iframe_html(fig, height=280, div_id="time-plot", include_plotlyjs="cdn"))

    @render.ui
    def table_ui():
        if loading():
            return ui.HTML('<div class="skeleton" style="height:200px;"></div>')
        stats = seg_stats()
        seg = segments()
        if stats is None or seg is None or not isinstance(stats, pd.DataFrame) or not isinstance(seg, pd.DataFrame):
            return ui.div("Load traffic to see most congested roads.", style="color: #64748b; padding: 1rem;")
        mode = (input.data_mode() or "hourly").strip().lower()
        TOP_N = 15

        if mode == "hourly":
            seg_cols = ["segment_id", "street_name", "road_class", "capacity_vph"]
            if "length_m" in seg.columns:
                seg_cols.append("length_m")
            merge = stats.merge(seg[[c for c in seg_cols if c in seg.columns]], on="segment_id", how="left")
            merge = merge.sort_values("vc_ratio", ascending=False).head(TOP_N)
            merge["severity"] = merge["vc_ratio"].map(vc_severity_label)
            if "length_m" in merge.columns:
                merge["travel_time_sec"] = np.where(
                    (merge["mean_speed_kmh"].notna()) & (merge["mean_speed_kmh"] > 0),
                    (merge["length_m"] / 1000) / (merge["mean_speed_kmh"] / 3600),
                    np.nan,
                )
            rows = []
            for _, row in merge.iterrows():
                vc_str = f"{row['vc_ratio']:.2f}" if pd.notna(row["vc_ratio"]) else "—"
                flow_str = f"{row['mean_flow_vph']:.0f}" if pd.notna(row["mean_flow_vph"]) else "—"
                cap_str = f"{row['capacity_vph']:.0f}" if pd.notna(row["capacity_vph"]) else "—"
                speed_str = f"{row['mean_speed_kmh']:.1f}" if pd.notna(row["mean_speed_kmh"]) else "—"
                tt = row.get("travel_time_sec")
                tt_str = f"{tt:.0f}" if pd.notna(tt) and np.isfinite(tt) else "—"
                sid = str(row.get("segment_id", ""))
                name = row.get("street_name") or sid
                cls = row.get("road_class", "")
                sev = row.get("severity", "")
                rows.append(f"<tr><td>{name}</td><td>{flow_str}</td><td>{cap_str}</td><td>{vc_str}</td><td>{speed_str}</td><td>{tt_str}</td><td>{sev}</td></tr>")
            thead = "<thead><tr><th>Road</th><th>Traffic Volume</th><th>Road Capacity</th><th>Congestion Level</th><th>Avg Speed (km/h)</th><th>Avg Travel Time (sec)</th><th>Traffic Status</th></tr></thead>"
        else:
            merge = stats.merge(seg[["segment_id", "street_name"]], on="segment_id", how="left")
            merge["street_name"] = merge["street_name"].fillna("").astype(str).replace("", "(unnamed)")
            vc_col = "peak_vc" if "peak_vc" in merge.columns else "vc_ratio"
            by_street = merge.groupby("street_name").agg(
                peak_vc=(vc_col, "max"),
                mean_speed=("mean_speed_kmh", "mean"),
                mean_flow=("mean_flow_vph", "mean"),
                n_segments=("segment_id", "count"),
            ).reset_index()
            by_street = by_street.sort_values("peak_vc", ascending=False).head(TOP_N)
            by_street["severity"] = by_street["peak_vc"].map(vc_severity_label)
            rows = []
            for _, row in by_street.iterrows():
                vc_str = f"{row['peak_vc']:.2f}" if pd.notna(row["peak_vc"]) else "—"
                flow_str = f"{row['mean_flow']:.0f}" if pd.notna(row["mean_flow"]) else "—"
                n_str = str(int(row["n_segments"]))
                name = row["street_name"]
                sev = row["severity"]
                rows.append(f"<tr><td>{name}</td><td>{vc_str}</td><td>{flow_str}</td><td>{n_str}</td><td>{sev}</td></tr>")
            thead = "<thead><tr><th>Road</th><th>Congestion Level</th><th>Traffic Volume</th><th>Segments</th><th>Traffic Status</th></tr></thead>"
        return ui.HTML(f'<table class="table table-sm">{thead}<tbody>{"".join(rows)}</tbody></table>')

    @render.ui
    def loading_status_ui():
        if loading():
            msg = loading_message.get() or "Loading…"
            return ui.div(
                ui.div(msg, {"class": "text-primary", "style": "font-size: 0.875rem; font-weight: 500;"}),
                ui.div(ui.HTML('<span class="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></span>'), {"class": "mt-1"}),
                style="padding: 0.25rem 0;",
            )
        return ui.div(
            ui.div("Ready — click Load traffic to fetch data", {"class": "text-muted", "style": "font-size: 0.8rem;"}),
            ui.div("Same date/time uses cache.", {"class": "text-muted", "style": "font-size: 0.7rem; margin-top: 0.25rem;"}),
        )

    OLLAMA_SYSTEM_PROMPT = """You are a friendly traffic analyst. Your summary will be read by residents, visitors, and city staff in a web app. Write in plain, conversational English but be informative.

Rules:
- Report only factual information from the data provided. Do not invent streets, conditions, or comparisons. Only mention streets that appear in the data the user gives you. Describe how bad congestion is (e.g. "heavily congested," "speeds in the 30s") based only on what the data shows for those streets.
- Do not use markdown (no **, __, #, backticks). No emoji. No chatty openers like "Here's a summary" or "Certainly."
- Break the response into multiple short sections. Do NOT output one long paragraph. Use blank lines between sections.
- Name specific streets as hotspots only if they are in the provided data. For each, give 1-2 sentences in plain language (no segment IDs, no raw v/c or vph numbers).
- Plain text only. Short paragraphs. Each hotspot gets its own short block."""

    @reactive.Effect
    @reactive.event(input.ai_analysis_btn)
    def _run_ai_summary():
        ai_loading.set(True)
        ai_summary_text.set("")
        ui.update_action_button("ai_analysis_btn", disabled=True, session=session)
        try:
            stats = seg_stats()
            seg = segments()
            obs = observations()
            mode = (input.data_mode() or "hourly").strip().lower()
            date_val = input.date()
            date_str = str(date_val)[:10] if date_val else "—"
            hour_val = input.hour() if mode == "hourly" else None

            kpi_speed = "—"
            kpi_flow = "—"
            kpi_congested = "0"
            kpi_peak_hour = "—"
            if stats is not None and not stats.empty:
                if "mean_speed_kmh" in stats.columns:
                    s = stats["mean_speed_kmh"].replace(0, np.nan).mean()
                    kpi_speed = f"{s:.1f}" if pd.notna(s) else "—"
                if "mean_flow_vph" in stats.columns:
                    m = stats["mean_flow_vph"].mean()
                    kpi_flow = f"{m:.0f}" if pd.notna(m) else "—"
                kpi_congested = str(int((stats["vc_ratio"] >= 0.8).sum()))
            if mode == "daily" and obs is not None and not obs.empty and "timestamp" in obs.columns:
                try:
                    obs = obs.copy()
                    obs["hour"] = pd.to_datetime(obs["timestamp"], utc=False).dt.hour
                    agg = obs.groupby("hour")["flow_vph"].mean()
                    if not agg.empty and seg is not None and "capacity_vph" in seg.columns:
                        cap_avg = seg["capacity_vph"].mean()
                        if cap_avg and cap_avg > 0:
                            vc_by_hour = agg / cap_avg
                            kpi_peak_hour = f"{int(vc_by_hour.idxmax())}:00"
                except Exception:
                    pass

            if mode == "hourly":
                top_label = "Top 15 segments (hourly)"
                if stats is not None and seg is not None:
                    merge = stats.merge(seg[["segment_id", "street_name"]], on="segment_id", how="left")
                    merge = merge.sort_values("vc_ratio", ascending=False).head(15)
                    rows = merge.apply(
                        lambda r: {
                            "segment_id": str(r.get("segment_id", "")),
                            "street_name": str(r.get("street_name", "")),
                            "vc_ratio": round(float(r["vc_ratio"]), 2) if pd.notna(r.get("vc_ratio")) else None,
                            "mean_speed_kmh": round(float(r["mean_speed_kmh"]), 1) if pd.notna(r.get("mean_speed_kmh")) else None,
                            "mean_flow_vph": round(float(r["mean_flow_vph"]), 0) if pd.notna(r.get("mean_flow_vph")) else None,
                        },
                        axis=1,
                    ).tolist()
                else:
                    rows = []
            else:
                top_label = "Top 15 streets (daily, by peak V/C)"
                if stats is not None and seg is not None:
                    merge = stats.merge(seg[["segment_id", "street_name"]], on="segment_id", how="left")
                    merge["street_name"] = merge["street_name"].fillna("").astype(str).replace("", "(unnamed)")
                    vc_col = "peak_vc" if "peak_vc" in merge.columns else "vc_ratio"
                    by_street = merge.groupby("street_name").agg(
                        peak_vc=(vc_col, "max"),
                        mean_speed=("mean_speed_kmh", "mean"),
                        mean_flow=("mean_flow_vph", "mean"),
                        n_segments=("segment_id", "count"),
                    ).reset_index().sort_values("peak_vc", ascending=False).head(15)
                    rows = by_street.apply(
                        lambda r: {
                            "street_name": str(r["street_name"]),
                            "peak_vc": round(float(r["peak_vc"]), 2) if pd.notna(r.get("peak_vc")) else None,
                            "mean_speed": round(float(r["mean_speed"]), 1) if pd.notna(r.get("mean_speed")) else None,
                            "mean_flow": round(float(r["mean_flow"]), 0) if pd.notna(r.get("mean_flow")) else None,
                            "n_segments": int(r["n_segments"]),
                        },
                        axis=1,
                    ).tolist()
                else:
                    rows = []

            # Prompt: factual only (streets and conditions from this data); conversational; multiple short blocks.
            user_prompt = f"""Write a traffic summary for a web app. Use plain text only (no markdown). Be conversational but stick to the facts in the data below.

Important: Only mention streets that appear in the "{top_label}" list below. Do not add any street or condition that is not supported by this data. Describe congestion in plain language (e.g. "heavily congested," "speeds in the 30s") based only on the relative severity and numbers in this dataset—do not speculate or compare to other days. Do NOT mention segment IDs or raw numbers (v/c, vph) in your reply.

Do NOT output one long paragraph. Break the reply into clear sections with blank lines between them.

Data (this is the only source—only report on streets and conditions from here):
- Mode: {mode.capitalize()}, Date: {date_str}
{f'- Hour: {hour_val}' if mode == 'hourly' and hour_val is not None else ''}
- {top_label}: {rows}
- Network-wide: mean speed {kpi_speed} km/h, mean flow {kpi_flow} vph, {kpi_congested} congested segments
{f'- Peak congestion hour: {kpi_peak_hour}' if mode == 'daily' else ''}

Required structure (use blank lines between sections):
1. One short lead line (e.g. "Key hotspots for [date] at [time]:" or "Where congestion is worst today:").
2. Then list 3 to 5 specific streets as hotspots. For each street, write 1-2 sentences on what to expect (e.g. "Main Street. Heavy congestion; expect slow going through downtown." then a blank line, then "Eden Street. One of the busiest corridors; speeds drop into the 30s in several places."). Each hotspot = its own short block.
3. One short closing line with practical advice (e.g. "Allow extra time or consider alternate routes during peak times.").

Keep under 220 words. Use multiple short paragraphs; do not merge everything into one paragraph."""

            # Ollama Cloud: chat API expects messages list; no local Ollama.
            messages = [
                {"role": "system", "content": OLLAMA_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ]
            out = query_llm(messages, stream=False)
            out = sanitize_report_text(out) if out else ""
            ai_summary_text.set(out if out else "Summary unavailable.")
        except OllamaCloudError as e:
            ai_summary_text.set(f"Summary unavailable. ({e})")
        except Exception as e:
            err_msg = str(e)[:200] if str(e) else type(e).__name__
            print(f"[AI Summary] Error: {e}", flush=True)
            ai_summary_text.set(f"Summary unavailable. ({err_msg})")
        finally:
            ai_loading.set(False)
            ui.update_action_button("ai_analysis_btn", disabled=False, session=session)

    @render.ui
    def ai_summary_ui():
        if ai_loading.get():
            return ui.div(
                ui.HTML('<span class="spinner-border spinner-border-sm text-primary me-2" role="status"></span>'),
                "Generating summary…",
                style="padding: 0.75rem 0; color: #64748b;",
            )
        text = ai_summary_text.get() or ""
        if not text:
            return ui.div("Generate a summary of observations and recommendations.", style="color: #64748b; padding: 0.75rem 0; font-size: 0.9rem;")
        s = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        s = s.replace("\n\n", "<br/><br/>").replace("\n", "<br/>")
        return ui.div(
            ui.HTML(f'<div style="max-height: 220px; overflow-y: auto; padding: 0.5rem 0; font-size: 0.9rem; line-height: 1.6;">{s}</div>'),
        )

    # Visualization debug output (panel removed from UI; kept for debugging)
    @render.ui
    def viz_debug_ui():
        map_len = viz_debug_map_len.get()
        sample_path = viz_debug_sample_path.get() or ""
        gauge_val = viz_debug_gauge_value.get()
        color_sample = viz_debug_color_sample.get() or ""
        lines = [
            "1. Map layer: len(map_df) passed to PyDeck = " + (str(map_len) if map_len is not None else "(not set)"),
            "2. Geometry format: PyDeck PathLayer expects path = list of [lon, lat] pairs per row.",
            "   First 3 path rows (first 4 coords each):",
            sample_path,
            "3. Color pipeline (vc_ratio -> vc_to_color -> color). First 10 rows vc_ratio and color:",
            color_sample,
            "   Color must be list of 3 integers 0-255. PyDeck PathLayer uses get_color=\"color\".",
            "4. Gauge: value passed to gauge component = " + (str(gauge_val) if gauge_val is not None else "(not set)"),
            "5. Map connected via ui.output_ui(\"map_ui\"). Gauge via ui.output_ui(\"gauge_ui\").",
        ]
        text = "\n".join(lines)
        return ui.HTML(f'<pre class="viz-debug">{text.replace("<", "&lt;").replace(">", "&gt;")}</pre>')

    @reactive.Effect
    def _show_error():
        err = api_error.get()
        if err:
            ui.notification_show(err, type="error", duration=10)


app = App(app_ui, server)