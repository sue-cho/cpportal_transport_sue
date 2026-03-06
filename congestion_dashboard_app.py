"""
Congestion Pricing Impact Dashboard – Policy briefing for transit staff.
Sections: Policy Timeline, Policy Impact, Current Conditions.
"""
from datetime import date, datetime
from typing import Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from shiny import App, reactive, render, ui

from dashboard_constants import (
    ERP_ACRONYM,
    LONDON_REFERENCES,
    LONDON_TIMELINE_EVENTS,
    SINGAPORE_REFERENCES,
    SINGAPORE_TIMELINE_EVENTS,
)
from dashboard_data import (
    get_before_after_for_policy_singapore,
    get_current_charging_before_after_london,
    get_erp2_first_vs_latest_singapore,
    get_erp2_recent_comparison_singapore,
    get_london_series,
    get_latest_lta_snapshot,
    get_latest_tfl_snapshot,
    get_singapore_series,
    get_tfl_snapshot_summary_series,
)
from dashboard_map import get_zone_map_html
from ollama_recommendations import get_recommendations as get_ollama_recommendations

# Policy event color (orange); improvement green; congestion red; neutral gray
POLICY_MARKER_COLOR = "#e67e22"
IMPROVEMENT_COLOR = "#27ae60"
CONGESTION_COLOR = "#e74c3c"
NEUTRAL_COLOR = "#7f8c8d"


def _months_ago(d: date, months: int) -> date:
    """Return date `months` months before d (same day if possible)."""
    year, month = d.year, d.month
    month -= months
    while month <= 0:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    if month == 12:
        return date(year, 12, min(d.day, 31))
    return date(year, month, min(d.day, 28))


def _to_ym(v) -> str:
    """Normalize month to 'YYYY-MM' for range comparison."""
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m")
    if hasattr(v, "to_pydatetime"):
        return v.to_pydatetime().strftime("%Y-%m")
    s = str(v)
    return s[:7] if len(s) >= 7 else s


# Load Plotly once in head so all chart HTML (with include_plotlyjs=False) can render
PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.27.0.min.js"

app_ui = ui.page_fluid(
    ui.head_content(ui.tags.script(src=PLOTLY_CDN)),
    ui.tags.style(
        """
        .city-btn { margin: 4px; }
        .city-btn-disabled { opacity: 0.5; cursor: not-allowed; }
        .policy-timeline-wrap { display: flex; align-items: flex-end; gap: 0.5rem; flex-wrap: wrap;
            padding: 1rem 0; border-bottom: 2px solid #ecf0f1; margin-bottom: 1.5rem; }
        .policy-timeline-wrap .form-group { margin: 0; display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
        .policy-timeline-wrap label { margin: 0 0.5rem 0 0; font-weight: 600; color: #2c3e50; }
        .policy-marker { width: 12px; height: 32px; background: #e67e22; border-radius: 2px; margin: 0 0.25rem; }
        .policy-timeline-wrap input[type="radio"] { accent-color: #e67e22; }
        .policy-timeline-wrap .form-check { display: inline-flex; align-items: center; margin-right: 1rem; }
        .policy-timeline-wrap .form-check-label { cursor: pointer; font-size: 0.95rem; }
        .section-heading { font-size: 1.25rem; font-weight: 600; color: #2c3e50; margin: 1.5rem 0 0.75rem 0; }
        .narrative-headline { font-size: 1.15rem; color: #34495e; margin: 0.75rem 0 1rem 0; line-height: 1.5; }
        .kpi-card { padding: 1rem; border-radius: 8px; text-align: center; }
        .kpi-value { font-size: 1.5rem; font-weight: 700; }
        .kpi-label { font-size: 0.85rem; color: #7f8c8d; margin-top: 0.25rem; }
        """
    ),
    ui.row(
        ui.column(12, ui.h1("Congestion Pricing Policy Briefing")),
        ui.column(12, ui.p("Understand policy timelines, traffic impact, and current conditions.", class_="text-muted")),
    ),
    ui.row(
        ui.column(12, ui.h5("City", class_="mb-2")),
        ui.column(12,
            ui.div(
                ui.input_action_button("city_london", "London", class_="city-btn btn-primary"),
                ui.input_action_button("city_singapore", "Singapore", class_="city-btn btn-secondary"),
                ui.tags.button("NYC", class_="btn btn-outline-secondary city-btn city-btn-disabled", disabled=True, title="Coming soon"),
            ),
        ),
    ),
    ui.row(ui.column(12, ui.output_ui("city_panel"))),
    title="Congestion Pricing Dashboard",
)


# -----------------------------------------------------------------------------
# Server
# -----------------------------------------------------------------------------

def server(input, output, session):
    selected_city = reactive.Value("london")
    recs_value = reactive.Value([])

    @reactive.Effect
    def _set_city():
        if input.city_london() > 0:
            selected_city.set("london")
        if input.city_singapore() > 0:
            selected_city.set("singapore")

    @reactive.Calc
    def _city() -> str:
        return selected_city()

    @reactive.Calc
    def _timeline_events():
        """Timeline events for current city."""
        return LONDON_TIMELINE_EVENTS if _city() == "london" else SINGAPORE_TIMELINE_EVENTS

    @output
    @render.ui
    def city_panel():
        city = _city()
        return ui.TagList(
            ui.row(ui.column(12, ui.output_ui("timeline_section"))),
            ui.row(ui.column(12, ui.output_ui("narrative_headline"))),
            ui.row(ui.column(12, ui.h4("Policy Impact", class_="section-heading"))),
            ui.row(
                ui.column(4, ui.output_ui("kpi_baseline")),
                ui.column(4, ui.output_ui("kpi_current")),
                ui.column(4, ui.output_ui("kpi_change")),
            ),
            ui.row(ui.column(12, ui.output_ui("main_chart"))),
            ui.row(ui.column(12, ui.h4("Current Conditions", class_="section-heading"))),
            ui.row(ui.column(12, ui.output_ui("current_conditions_summary"))),
            ui.row(ui.column(12, ui.output_ui("current_conditions_chart"))),
            ui.row(ui.column(12, ui.card(ui.card_header("Zone map"), ui.output_ui("map_ui")))),
            ui.row(ui.column(12, ui.card(ui.card_header("Policy insights"), ui.input_action_button("gen_rec", "Generate insights"), ui.output_ui("recommendations_ui")))),
            ui.row(ui.column(12, ui.card(ui.card_header("Debug", class_="bg-light"), ui.output_ui("debug_panel")))),
            ui.tags.footer(ui.p(ERP_ACRONYM + ". Data: London Datastore, data.gov.sg, TfL, LTA.")),
        )

    def _selected_policy_event():
        """Legacy hook (no longer used)."""
        return None

    @output
    @render.ui
    def timeline_section():
        """Single-policy briefing: London (Jun 2020) and Singapore (ERP 2.0 May 2024)."""
        city = _city()
        if city == "london":
            return ui.div(
                ui.div(ui.span("Policy Impact — Current charging hours (Jun 2020)", class_="fw-bold me-2"), class_="mb-1"),
                ui.p("Chart shows data since 2010. Summary uses post-COVID baseline (Jun 2022–May 2023) vs last year to avoid COVID skew.", class_="small text-muted mb-0"),
                class_="policy-timeline-wrap",
            )
        return ui.div(
            ui.div(ui.span("Policy Impact — ERP 2.0 (May 2024)", class_="fw-bold me-2"), class_="mb-1"),
            ui.p("Summary compares 2023–2024 vs 2024–present to understand the impact since ERP 2.0 (May 2024).", class_="small text-muted mb-0"),
            class_="policy-timeline-wrap",
        )

    @output
    @render.ui
    def timeline_selected_description():
        """Show description of selected policy (Singapore only)."""
        return ui.div()

    @output
    @render.ui
    def narrative_headline():
        """Plain-language summary: policy impact and/or current conditions."""
        city = _city()
        try:
            if city == "london":
                first_avg, last_avg, pct, first_label, last_label = get_current_charging_before_after_london("confirmed_vehicles")
                if pct is not None:
                    direction = "lower" if pct < 0 else "higher"
                    return ui.p(
                        f"Vehicles entering the London CCZ per month are {abs(pct):.1f}% {direction} in the most recent year ({last_label}) than in the post-COVID baseline (Jun 2022–May 2023).",
                        class_="narrative-headline",
                    )
                return ui.p("Current charging hours began Jun 2020. Check data coverage.", class_="narrative-headline text-muted")
            # Singapore: ERP 2.0 (May 2024) reference policy – first full year after vs most recent year
            first_avg, latest_avg, pct, first_label, latest_label = get_erp2_recent_comparison_singapore()
            if pct is not None:
                direction = "lower" if pct < 0 else "higher"
                return ui.p(
                    f"Average daily traffic entering the city is {abs(pct):.1f}% {direction} in {latest_label} than in {first_label}.",
                    class_="narrative-headline",
                )
            return ui.p("ERP 2.0 (May 2024) is the reference policy. Check data coverage.", class_="narrative-headline text-muted")
        except Exception as e:
            return ui.p(f"Summary unavailable: {e}", class_="narrative-headline text-muted")

    @output
    @render.ui
    def kpi_baseline():
        city = _city()
        if city == "london":
            first_avg, last_avg, pct, first_label, last_label = get_current_charging_before_after_london("confirmed_vehicles")
            before_avg, before_label = first_avg, first_label
        else:
            first_avg, latest_avg, pct, first_label, latest_label = get_erp2_recent_comparison_singapore()
            before_avg, before_label = first_avg, first_label
        try:
            if before_avg is None:
                return ui.div(ui.div("—", class_="kpi-value"), ui.div("Post-COVID baseline", class_="kpi-label"), class_="card kpi-card border")
            val = f"{before_avg:,.0f}" if before_avg >= 1000 else f"{before_avg:,.1f}"
            kpi_label = f"Post-COVID baseline ({before_label})" if city == "london" else f"Before ({before_label})"
            return ui.div(ui.div(val, class_="kpi-value"), ui.div(kpi_label, class_="kpi-label"), class_="card kpi-card border")
        except Exception:
            return ui.div(ui.div("—", class_="kpi-value"), ui.div("Before policy", class_="kpi-label"), class_="card kpi-card border")

    @output
    @render.ui
    def kpi_current():
        city = _city()
        if city == "london":
            _, last_avg, _, _, last_label = get_current_charging_before_after_london("confirmed_vehicles")
            after_avg, after_label = last_avg, last_label
        else:
            _, latest_avg, _, _, latest_label = get_erp2_recent_comparison_singapore()
            after_avg, after_label = latest_avg, latest_label
        try:
            if after_avg is None:
                return ui.div(ui.div("—", class_="kpi-value"), ui.div("Most recent year", class_="kpi-label"), class_="card kpi-card border")
            val = f"{after_avg:,.0f}" if after_avg >= 1000 else f"{after_avg:,.1f}"
            kpi_label = f"Most recent year ({after_label})" if city == "london" else f"After ({after_label})"
            return ui.div(ui.div(val, class_="kpi-value"), ui.div(kpi_label, class_="kpi-label"), class_="card kpi-card border")
        except Exception:
            return ui.div(ui.div("—", class_="kpi-value"), ui.div("After policy", class_="kpi-label"), class_="card kpi-card border")

    @output
    @render.ui
    def kpi_change():
        city = _city()
        if city == "london":
            _, _, pct, _, _ = get_current_charging_before_after_london("confirmed_vehicles")
        else:
            _, _, pct, _, _ = get_erp2_recent_comparison_singapore()
        try:
            if pct is None:
                return ui.div(ui.div("—", class_="kpi-value"), ui.div("Change", class_="kpi-label"), class_="card kpi-card border")
            color = IMPROVEMENT_COLOR if pct < 0 else (CONGESTION_COLOR if pct > 0 else NEUTRAL_COLOR)
            change_label = "Change (most recent vs post-COVID baseline)" if city == "london" else "Change (after vs before)"
            return ui.div(
                ui.div(f"{pct:+.1f}%", style=f"color: {color};", class_="kpi-value"),
                ui.div(change_label, class_="kpi-label"),
                class_="card kpi-card border",
            )
        except Exception:
            return ui.div(ui.div("—", class_="kpi-value"), ui.div("Change", class_="kpi-label"), class_="card kpi-card border")

    @output
    @render.ui
    def erp2_impact_card():
        """London: Current charging hours (Jun 2020) policy impact. Card not in layout; kept for consistency."""
        if _city() != "london":
            return ui.div()
        try:
            baseline_avg, last_avg, pct, baseline_label, last_label = get_current_charging_before_after_london("confirmed_vehicles")
            if baseline_avg is None and last_avg is None:
                return ui.div()
            lines = [ui.p(ui.strong("Policy trend (post-COVID baseline)"), class_="mb-2")]
            lines.append(ui.p(f"Post-COVID baseline: {baseline_avg:,.0f} vehicles/month" if baseline_avg is not None else "Baseline: —", class_="small mb-1"))
            lines.append(ui.p(f"({baseline_label})", class_="small text-muted mb-1"))
            lines.append(ui.p(f"Most recent year: {last_avg:,.0f} vehicles/month" if last_avg is not None else "Most recent: —", class_="small mb-1"))
            lines.append(ui.p(f"({last_label})", class_="small text-muted mb-1"))
            if pct is not None:
                lines.append(ui.p(f"Change: {pct:+.1f}%", class_="small mb-0"))
            return ui.card(ui.card_header("Policy Impact — Current charging hours (Jun 2020)"), *lines)
        except Exception:
            return ui.div()

    def _severity_badge_class(severity: str) -> str:
        """Bootstrap badge class by TfL-style severity."""
        s = (severity or "").lower()
        if "good" in s or "no" in s:
            return "bg-success"
        if "minor" in s or "delays" in s:
            return "bg-warning text-dark"
        if "serious" in s or "closure" in s or "severe" in s:
            return "bg-danger"
        return "bg-secondary"

    def _severity_to_category(severity: str) -> str:
        """Map TfL status_severity to: Good, moderate delays, serious congestion."""
        s = (severity or "").lower()
        if "good" in s or "no" in s or "normal" in s:
            return "Good"
        if "minor" in s or "delays" in s or "moderate" in s:
            return "moderate delays"
        if "serious" in s or "closure" in s or "severe" in s:
            return "serious congestion"
        return "other"

    @output
    @render.ui
    def current_conditions_summary():
        """Interpreted snapshot: London (status %) or Singapore (speed band narrative)."""
        city = _city()
        try:
            if city == "london":
                df = get_latest_tfl_snapshot(limit=500)
                if df.empty:
                    return ui.card(ui.card_header("Current Conditions (Latest Snapshot)"), ui.p("No TfL snapshot. Run log_tfl_snapshot.py.", class_="text-muted mb-0"))
                ts = df["snapshot_time"].iloc[0] if "snapshot_time" in df.columns else ""
                ts_str = ts.strftime("%Y-%m-%d %H:%M UTC") if hasattr(ts, "strftime") and pd.notna(ts) else str(ts)
                total = len(df)
                lines = [ui.p(ui.strong("Current Conditions (Latest Snapshot)"), " — ", ts_str, class_="mb-2")]
                if "status_severity" in df.columns and total > 0:
                    df_cat = df.copy()
                    df_cat["_cat"] = df_cat["status_severity"].map(_severity_to_category)
                    pcts = df_cat["_cat"].value_counts(normalize=True).mul(100)
                    good_pct = pcts.get("Good", 0) + pcts.get("good", 0)
                    mod_pct = pcts.get("moderate delays", 0)
                    serious_pct = pcts.get("serious congestion", 0)
                    other_pct = pcts.get("other", 0)
                    good_pct = float(good_pct) if hasattr(good_pct, "__float__") else 0
                    mod_pct = float(mod_pct) if hasattr(mod_pct, "__float__") else 0
                    serious_pct = float(serious_pct) if hasattr(serious_pct, "__float__") else 0
                    if good_pct >= 50:
                        lead = "Most London corridors currently show normal traffic flow."
                    elif serious_pct >= 20:
                        lead = "London corridors are experiencing notable congestion."
                    else:
                        lead = "London corridors show mixed traffic conditions."
                    lines.append(ui.p(lead, class_="mb-1"))
                    lines.append(ui.p(f"{good_pct:.0f}% of monitored segments are classified as Good.", class_="small mb-1"))
                    lines.append(ui.p(f"{mod_pct:.0f}% show moderate delays.", class_="small mb-1"))
                    lines.append(ui.p(f"{serious_pct:.0f}% show serious congestion.", class_="small mb-0"))
                else:
                    lines.append(ui.p("Status breakdown unavailable (missing status_severity).", class_="small text-muted mb-0"))
                return ui.card(ui.card_header("Current Conditions (Latest Snapshot)"), *lines)
            else:
                df = get_latest_lta_snapshot(limit=5000)
                if df.empty:
                    return ui.card(ui.card_header("Current Conditions (Latest Snapshot)"), ui.p("No LTA snapshot. Run log_lta_speed_snapshot.py.", class_="text-muted mb-0"))
                ts = df["snapshot_time"].iloc[0] if "snapshot_time" in df.columns else ""
                ts_str = ts.strftime("%Y-%m-%d %H:%M UTC") if hasattr(ts, "strftime") and pd.notna(ts) else str(ts)
                total = len(df)
                lines = [ui.p(ui.strong("Current Conditions (Latest Snapshot)"), " — ", ts_str, class_="mb-1"), ui.p(f"{total} monitored road segments.", class_="mb-2 small text-muted")]
                if "speed_band" in df.columns:
                    sb = df["speed_band"].dropna().astype(int)
                    if len(sb):
                        counts = sb.value_counts(normalize=True).mul(100)
                        n = len(sb)
                        pct_b1 = float(counts.get(1, 0)) if 1 in counts.index else 0
                        pct_mid = sum(float(counts.get(b, 0)) for b in [2, 3, 4] if b in counts.index)
                        pct_slow = sum(float(counts.get(b, 0)) for b in counts.index if b >= 5)
                        if pct_mid >= 40:
                            lead = "Most monitored road segments currently show moderate traffic speeds (bands 2–4)."
                        elif pct_b1 >= 30:
                            lead = "A substantial share of segments show faster flow (Band 1)."
                        else:
                            lead = "Traffic speeds are mixed across monitored segments."
                        lines.append(ui.p(lead, class_="mb-1"))
                        lines.append(ui.p(f"{pct_b1:.0f}% are in Band 1 (fastest). {pct_mid:.0f}% in bands 2–4 (moderate). {pct_slow:.0f}% in Band 5+ (slower).", class_="small mb-0"))
                    else:
                        lines.append(ui.p("No speed band values in snapshot.", class_="small text-muted mb-0"))
                else:
                    lines.append(ui.p("Speed band data unavailable.", class_="small text-muted mb-0"))
                return ui.card(ui.card_header("Current Conditions (Latest Snapshot)"), *lines)
        except Exception as e:
            return ui.card(ui.card_header("Current conditions"), ui.p(f"Summary unavailable: {e}", class_="text-muted mb-0"))

    @output
    @render.ui
    def current_conditions_chart():
        """London: corridor status over recent snapshots. Singapore: speed band distribution (latest)."""
        city = _city()
        try:
            if city == "london":
                df = get_tfl_snapshot_summary_series(limit_snapshots=24)
                if df.empty:
                    return ui.p("No TfL snapshot history. Run log_tfl_snapshot.py repeatedly to build history.", class_="text-muted")
                if len(df["snapshot_time"].unique()) < 2:
                    return ui.p("Need at least 2 snapshot times for trend chart. Run log_tfl_snapshot.py again.", class_="text-muted")
                df = df.copy()
                df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
                fig = px.bar(
                    df, x="snapshot_time", y="n", color="status_severity",
                    labels={"snapshot_time": "Snapshot time", "n": "Corridors", "status_severity": "Status"},
                    title="Road status over recent snapshots",
                )
                fig.update_layout(barmode="stack", template="plotly_white", height=220, margin=dict(t=40, b=40))
                return ui.HTML(fig.to_html(include_plotlyjs=False, config={"displayModeBar": True}))
            else:
                df = get_latest_lta_snapshot(limit=2000)
                if df.empty:
                    return ui.p("No LTA snapshot. Run log_lta_speed_snapshot.py.", class_="text-muted")
                if "speed_band" not in df.columns:
                    return ui.p("LTA data missing speed_band column. Check API response.", class_="text-muted")
                sb = df["speed_band"].dropna()
                if sb.empty:
                    return ui.p("No speed band values in snapshot.", class_="text-muted")
                counts = sb.astype(int).value_counts().sort_index().reset_index()
                counts.columns = ["speed_band", "segments"]
                fig = px.bar(
                    counts, x="speed_band", y="segments",
                    labels={"speed_band": "Speed band (1=fastest)", "segments": "Segments"},
                    title="Speed band distribution (latest snapshot)",
                )
                fig.update_layout(template="plotly_white", height=220, margin=dict(t=40, b=40))
                return ui.HTML(fig.to_html(include_plotlyjs=False, config={"displayModeBar": True}))
        except Exception as e:
            return ui.p(f"Chart error: {e}", class_="text-danger")

    @output
    @render.ui
    def main_chart():
        """Historical traffic: London (current charging Jun 2020 marker) or Singapore with policy markers."""
        city = _city()
        try:
            if city == "london":
                df = get_london_series(metric_type="confirmed_vehicles", start_month="2010-01")
                if df.empty or "month" not in df.columns or "vehicles" not in df.columns:
                    return ui.p("No London data. Run ingest_london_ccz.py.")
                df = df.copy()
                df["month"] = pd.to_datetime(df["month"])
                df = df.sort_values("month")
                if len(df) == 0:
                    return ui.p("No London data to plot.", class_="text-muted")
                # Use ms since epoch so Plotly never does int + datetime
                x_vals = [float(pd.Timestamp(t).value) / 1e6 for t in df["month"]]
                policy_ms = float(pd.Timestamp(datetime(2020, 6, 1)).value) / 1e6
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=x_vals, y=df["vehicles"].tolist(), mode="lines+markers", name="Vehicles"))
                fig.add_vline(x=policy_ms, line_dash="dash", annotation_text="Current charging hours (Jun 2020)")
                fig.update_layout(
                    title="Vehicles entering London CCZ per month",
                    template="plotly_white",
                    height=320,
                    xaxis=dict(type="date", tickformat="%b %Y"),
                )
            else:
                df = get_singapore_series()
                if df.empty:
                    return ui.p("No Singapore data available (DB empty and data.gov.sg fallback unavailable).", class_="text-muted")
                if "year" not in df.columns or "avg_daily_vehicles" not in df.columns:
                    return ui.p("Singapore data missing year or avg_daily_vehicles column.", class_="text-muted")
                d = df.copy()
                d["year"] = pd.to_numeric(d["year"], errors="coerce")
                d["avg_daily_vehicles"] = pd.to_numeric(d["avg_daily_vehicles"], errors="coerce")
                d = d.dropna(subset=["year", "avg_daily_vehicles"]).sort_values("year")
                if len(d) == 0:
                    return ui.p("No valid Singapore data to plot.", class_="text-muted")
                x_dates = pd.to_datetime(d["year"].astype(int).astype(str) + "-01-01")
                # Use ms since epoch so Plotly never does Timestamp ± int
                x_ms = [float(pd.Timestamp(t).value) / 1e6 for t in x_dates]
                policy_ms = float(pd.Timestamp("2024-05-01").value) / 1e6
                fig = go.Figure()
                fig.add_scatter(
                    x=x_ms,
                    y=d["avg_daily_vehicles"].astype(float).tolist(),
                    mode="lines+markers",
                    name="Avg daily vehicles",
                )
                fig.add_vline(
                    x=policy_ms,
                    line_dash="dash",
                    line_color=POLICY_MARKER_COLOR,
                    line_width=2,
                    annotation_text="ERP 2.0 (May 2024)",
                    annotation_position="top",
                )
                fig.update_layout(
                    title="Avg daily traffic entering the city (Singapore)",
                    template="plotly_white",
                    height=320,
                    xaxis=dict(type="date", tickformat="%Y"),
                    yaxis_title="Avg daily vehicles",
                )
            return ui.HTML(fig.to_html(include_plotlyjs=False, config={"displayModeBar": True}))
        except Exception as e:
            return ui.p(f"Chart error: {e}")

    @reactive.Effect
    @reactive.event(input.gen_rec)
    def _gen_rec():
        city = _city()
        try:
            if city == "london":
                _, _, pct, baseline_label, last_label = get_current_charging_before_after_london("confirmed_vehicles")
                recs = get_ollama_recommendations(
                    city=city.capitalize(),
                    reference_label="Current charging hours (Jun 2020)",
                    baseline_summary=baseline_label,
                    comparison_summary=last_label,
                    pct_change=pct,
                )
                recs_value.set(recs)
            else:
                _, _, pct, first_label, latest_label = get_erp2_recent_comparison_singapore()
                recs = get_ollama_recommendations(
                    city=city.capitalize(),
                    reference_label="ERP 2.0 (May 2024)",
                    baseline_summary=first_label,
                    comparison_summary=latest_label,
                    pct_change=pct,
                )
                recs_value.set(recs)
        except Exception as e:
            recs_value.set([str(e)])

    @output
    @render.ui
    def recommendations_ui():
        recs = recs_value()
        if not recs:
            return ui.p("Click 'Generate recommendations' to get AI suggestions (requires Ollama running).")
        return ui.TagList(ui.tags.ul(*[ui.tags.li(r) for r in recs]))

    @output
    @render.ui
    def debug_panel():
        """Debug tab: dataset previews, row counts, API status, last refresh."""
        from datetime import datetime as dt
        ts = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        parts = [ui.p(ui.strong("Last refresh: "), ts, class_="small mb-2")]
        try:
            df_london = get_london_series(metric_type="confirmed_vehicles")
            df_sg = get_singapore_series()
            df_tfl = get_latest_tfl_snapshot(limit=5)
            df_lta = get_latest_lta_snapshot(limit=5)
            df_tfl_series = get_tfl_snapshot_summary_series(limit_snapshots=24)
            parts.append(ui.h6("London CCZ monthly (confirmed_vehicles)"))
            if not df_london.empty and "month" in df_london.columns:
                d = df_london.copy()
                d["month"] = pd.to_datetime(d["month"], utc=False, errors="coerce")
                d = d.dropna(subset=["month"])
                if len(d) > 0:
                    range_min, range_max = d["month"].min(), d["month"].max()
                    parts.append(ui.p(ui.strong("Data range: "), f"{range_min.strftime('%Y-%m')} to {range_max.strftime('%Y-%m')} ({len(d)} months). ", "Source: confirmed_vehicles typically from Oct 2016 (London Datastore).", class_="small text-info"))
            parts.append(ui.p(f"Rows: {len(df_london)}. Columns: {list(df_london.columns) if not df_london.empty else []}", class_="small"))
            if not df_london.empty and "month" in df_london.columns and "vehicles" in df_london.columns:
                d = df_london.copy()
                d["month"] = pd.to_datetime(d["month"], utc=False, errors="coerce")
                d = d.dropna(subset=["month"]).sort_values("month")
                d["vehicles"] = pd.to_numeric(d["vehicles"], errors="coerce")
                d = d.dropna(subset=["vehicles"])
                parts.append(ui.p(ui.strong("London chart data (all): "), f"Rows: {len(d)}, min_date: {d['month'].min()}, max_date: {d['month'].max()}", class_="small text-info"))
                parts.append(ui.output_data_frame("debug_london_filtered_head"))
            parts.append(ui.h6("London CCZ raw (first 5 rows)"))
            parts.append(ui.output_data_frame("debug_london_head"))
            parts.append(ui.h6("Singapore city annual"))
            parts.append(ui.p(f"Rows: {len(df_sg)}. Columns: {list(df_sg.columns) if not df_sg.empty else []}", class_="small"))
            parts.append(ui.output_data_frame("debug_sg_head"))
            parts.append(ui.h6("TfL road status (latest snapshot, 5 rows). Expected: snapshot_time, status_severity"))
            parts.append(ui.p(f"Rows: {len(df_tfl)}. Columns: {list(df_tfl.columns) if not df_tfl.empty else []}", class_="small"))
            parts.append(ui.output_data_frame("debug_tfl_head"))
            parts.append(ui.h6("LTA speed bands (latest snapshot, 5 rows). Expected: snapshot_time, speed_band, road_segment_id"))
            parts.append(ui.p(f"Rows: {len(df_lta)}. Columns: {list(df_lta.columns) if not df_lta.empty else []}", class_="small"))
            parts.append(ui.output_data_frame("debug_lta_head"))
            parts.append(ui.h6("TfL snapshot summary (for London trend chart)"))
            n_uniq = df_tfl_series["snapshot_time"].nunique() if not df_tfl_series.empty and "snapshot_time" in df_tfl_series.columns else 0
            parts.append(ui.p(f"Rows: {len(df_tfl_series)}. Unique snapshot_times: {n_uniq}", class_="small"))
            return ui.TagList(*parts)
        except Exception as e:
            parts.append(ui.p(ui.strong("Error: "), str(e), class_="text-danger small"))
            return ui.TagList(*parts)

    @output
    @render.data_frame
    def debug_london_filtered_head():
        """Dataframe passed to the London chart (all months, sorted)."""
        df = get_london_series(metric_type="confirmed_vehicles")
        if df.empty or "month" not in df.columns or "vehicles" not in df.columns:
            return render.DataGrid(pd.DataFrame(), height="80px")
        df = df.copy()
        df["month"] = pd.to_datetime(df["month"], utc=False, errors="coerce")
        df = df.dropna(subset=["month"]).sort_values("month")
        df["vehicles"] = pd.to_numeric(df["vehicles"], errors="coerce")
        df = df.dropna(subset=["vehicles"])
        return render.DataGrid(df.head(10), height="150px")

    @output
    @render.data_frame
    def debug_london_head():
        df = get_london_series(metric_type="confirmed_vehicles")
        return render.DataGrid(df.head(5), height="120px")

    @output
    @render.data_frame
    def debug_sg_head():
        df = get_singapore_series()
        return render.DataGrid(df.head(5), height="120px")

    @output
    @render.data_frame
    def debug_tfl_head():
        df = get_latest_tfl_snapshot(limit=5)
        return render.DataGrid(df, height="120px")

    @output
    @render.data_frame
    def debug_lta_head():
        df = get_latest_lta_snapshot(limit=5)
        return render.DataGrid(df, height="120px")

    @output
    @render.ui
    def map_ui():
        city = _city()
        if city == "nyc":
            return ui.p("Map for NYC coming later.")
        try:
            html = get_zone_map_html(city)
            return ui.HTML(html)
        except Exception as e:
            return ui.p(f"Map unavailable: {e}")


app = App(app_ui, server)
