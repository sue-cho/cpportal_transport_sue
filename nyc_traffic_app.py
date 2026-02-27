from datetime import date
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from shiny import App, reactive, render, ui

from nyc_traffic_data import build_aggregates, fetch_traffic_data


# -----------------------------------------------------------------------------
# Data loading and preparation
# -----------------------------------------------------------------------------

PRICING_START = pd.Timestamp("2025-01-01")

# Year window for summary comparisons (used in numeric stats only).
YEAR_START = 2024
YEAR_END = 2025

# Fetch a wider year range so that plots still show data even if
# the dataset does not yet contain 2024/2025 observations.
FETCH_START_YEAR = 2010
FETCH_END_YEAR = 2025

raw_df = fetch_traffic_data(start_year=FETCH_START_YEAR, end_year=FETCH_END_YEAR)
daily_df, _monthly_unused = build_aggregates(
    raw_df, pricing_start=str(PRICING_START.date())
)

if daily_df.empty:
    BORO_CHOICES = ["All"]
else:
    BORO_CHOICES = ["All"] + sorted(
        b for b in daily_df["boro"].dropna().astype(str).unique()
    )


def _compute_citywide_daily_stats() -> Tuple[
    Optional[float], Optional[float], Optional[float]
]:
    """Return (avg_2024, avg_2025, pct_change) for citywide daily volumes."""
    if daily_df.empty:
        return None, None, None

    city = (
        daily_df.groupby("date", as_index=False)["daily_volume"]
        .sum()
        .rename(columns={"daily_volume": "city_daily_volume"})
    )
    city["year"] = city["date"].dt.year

    avg_2024 = city.loc[city["year"] == 2024, "city_daily_volume"].mean()
    avg_2025 = city.loc[city["year"] == 2025, "city_daily_volume"].mean()

    pct_change = None
    if pd.notna(avg_2024) and avg_2024 > 0 and pd.notna(avg_2025):
        pct_change = (avg_2025 - avg_2024) / avg_2024 * 100.0

    return (
        float(avg_2024) if pd.notna(avg_2024) else None,
        float(avg_2025) if pd.notna(avg_2025) else None,
        float(pct_change) if pct_change is not None else None,
    )


def _compute_peak_change_pct() -> Optional[float]:
    """
    Compute percent change in average daily peak-hours volume (2025 vs 2024), citywide.

    Peak hours are approximated from MTA congestion pricing documentation:
      - Weekdays: 05:00–21:00
      - Weekends: 09:00–21:00
    """
    if raw_df.empty:
        return None

    df = raw_df.copy()
    if "date" not in df.columns:
        return None

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "hh", "vol"])

    df["hh"] = pd.to_numeric(df["hh"], errors="coerce")
    df = df.dropna(subset=["hh"])

    df["is_weekday"] = df["date"].dt.weekday < 5
    weekday_peak = df["is_weekday"] & (df["hh"] >= 5) & (df["hh"] < 21)
    weekend_peak = ~df["is_weekday"] & (df["hh"] >= 9) & (df["hh"] < 21)

    df_peak = df[weekday_peak | weekend_peak]
    if df_peak.empty:
        return None

    daily_peak = (
        df_peak.groupby("date", as_index=False)["vol"]
        .sum()
        .rename(columns={"vol": "peak_daily_volume"})
    )
    daily_peak["year"] = daily_peak["date"].dt.year

    avg_2024 = daily_peak.loc[daily_peak["year"] == 2024, "peak_daily_volume"].mean()
    avg_2025 = daily_peak.loc[daily_peak["year"] == 2025, "peak_daily_volume"].mean()

    if pd.notna(avg_2024) and avg_2024 > 0 and pd.notna(avg_2025):
        return (float(avg_2025) - float(avg_2024)) / float(avg_2024) * 100.0

    return None


AVG_DAILY_2024, AVG_DAILY_2025, PCT_CHANGE_DAILY = _compute_citywide_daily_stats()
PCT_CHANGE_PEAK = _compute_peak_change_pct()


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.h2("NYC Borough Traffic & Congestion Pricing"),
        ui.p(
            "Automated Traffic Volume Counts from NYC Open Data, "
            "aggregated to daily (7-day rolling average) and monthly trends around "
            f"congestion pricing (reference date: {PRICING_START.date().isoformat()}) "
            "for all NYC boroughs. Daily trends are shown for 2024-01-01 to 2025-01-01; "
            "monthly volumes compare 2024 and 2025 to reveal seasonal patterns."
        ),
        ui.input_select(
            "boro",
            "Borough",
            choices=BORO_CHOICES,
            selected="All",
        ),
        ui.hr(),
        ui.p(
            "Data source: ",
            ui.a(
                "Automated Traffic Volume Counts (7ym2-wayt)",
                href=(
                    "https://data.cityofnewyork.us/Transportation/"
                    "Automated-Traffic-Volume-Counts/7ym2-wayt"
                ),
                target="_blank",
            ),
        ),
        width=320,
    ),
    ui.page_fillable(
        ui.layout_columns(
            ui.card(
                ui.card_header("Daily Traffic Volume Trend (7-day Rolling Avg)"),
                ui.output_ui("daily_trend"),
            ),
            ui.card(
                ui.card_header("Monthly Volume 2024 vs 2025 (+ % Change)"),
                ui.output_ui("monthly_comparison"),
            ),
            col_widths=(6, 6),
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Summary Statistics"),
                ui.output_ui("summary_stats"),
            ),
            col_widths=(12,),
        ),
    ),
    title="NYC Traffic Volume – Congestion Pricing",
)


# -----------------------------------------------------------------------------
# Server logic
# -----------------------------------------------------------------------------

def server(input, output, session):
    @reactive.Calc
    def _filtered_daily() -> pd.DataFrame:
        df = daily_df.copy()
        if df.empty:
            return df

        # 7-day rolling average per borough to smooth noise.
        df = df.sort_values(["boro", "date"])
        df["rolling_7d"] = (
            df.groupby("boro")["daily_volume"]
            .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
        )

        boro = input.boro()
        if boro and boro != "All":
            df = df[df["boro"] == boro]

        return df.sort_values("date")

    @reactive.Calc
    def _filtered_monthly() -> pd.DataFrame:
        """
        Citywide monthly volume aggregated across all boroughs.
        Prefer 2024/2025 if present, otherwise fall back to all available years.
        """
        if daily_df.empty:
            return daily_df

        df = daily_df.copy()
        df["year"] = df["date"].dt.year
        df["calendar_month"] = df["date"].dt.month
        df_24_25 = df[df["year"].isin([2024, 2025])]

        if df_24_25.empty:
            # If there is no data for 2024/2025, fall back to all available years
            base_df = df
        else:
            base_df = df_24_25

        monthly = (
            base_df.groupby(["year", "calendar_month"], as_index=False)[
                "daily_volume"
            ]
            .sum()
            .rename(columns={"daily_volume": "monthly_volume"})
        )

        return monthly

    @output
    @render.ui
    def daily_trend():
        df = _filtered_daily()
        if df.empty:
            return ui.p("No data available for selected filters.")

        fig = px.line(
            df,
            x="date",
            y="rolling_7d",
            color="boro",
            labels={
                "date": "Date",
                "rolling_7d": "7-day Rolling Avg Daily Volume",
                "boro": "Borough",
            },
            template="plotly_white",
        )

        # Reference line at congestion pricing start
        fig.add_vline(
            x=PRICING_START,
            line_width=2,
            line_dash="dash",
            line_color="crimson",
        )
        fig.add_annotation(
            x=PRICING_START,
            y=df["rolling_7d"].max() if not df["rolling_7d"].empty else 0,
            text=f"Pricing start {PRICING_START.date().isoformat()}",
            showarrow=True,
            arrowhead=1,
            ay=-40,
        )

        fig.update_layout(
            legend_title_text="Borough",
            margin=dict(l=40, r=20, t=60, b=40),
        )

        # Render interactive Plotly chart as HTML without relying on anywidget.
        html = fig.to_html(include_plotlyjs="cdn", full_html=False)
        return ui.HTML(html)

    @output
    @render.ui
    def monthly_comparison():
        df = _filtered_monthly()
        if df.empty:
            return ui.p("No data available for selected filters.")

        month_names = {
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
        }
        df = df.copy()
        df["month_label"] = df["calendar_month"].map(month_names)

        # Prepare percent change per month (2025 vs 2024).
        pivot = df.pivot(
            index="calendar_month",
            columns="year",
            values="monthly_volume",
        )
        pct_df = None
        if 2024 in pivot.columns and 2025 in pivot.columns:
            pct_series = (pivot[2025] - pivot[2024]) / pivot[2024] * 100.0
            pct_df = (
                pct_series.replace([np.inf, -np.inf], np.nan)
                .dropna()
                .reset_index()
            )
            pct_df["month_label"] = pct_df["calendar_month"].map(month_names)

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        for year, color in [(2024, "#1f77b4"), (2025, "#ff7f0e")]:
            sub = df[df["year"] == year]
            if sub.empty:
                continue
            fig.add_bar(
                x=sub["month_label"],
                y=sub["monthly_volume"],
                name=str(year),
                offsetgroup=str(year),
                marker_color=color,
                secondary_y=False,
            )

        if pct_df is not None and not pct_df.empty:
            fig.add_scatter(
                x=pct_df["month_label"],
                y=pct_df[0],
                name="% change (2025 vs 2024)",
                mode="lines+markers",
                marker_color="#2ca02c",
                secondary_y=True,
            )

        fig.update_layout(
            barmode="group",
            template="plotly_white",
            margin=dict(l=40, r=20, t=60, b=40),
            legend_title_text="Year / Metric",
        )
        fig.update_yaxes(
            title_text="Monthly Vehicle Volume", secondary_y=False
        )
        fig.update_yaxes(
            title_text="Percent Change (%)", secondary_y=True
        )

        html = fig.to_html(include_plotlyjs="cdn", full_html=False)
        return ui.HTML(html)

    @output
    @render.ui
    def summary_stats():
        if daily_df.empty:
            return ui.p("No data available for summary statistics.")

        def fmt(value: Optional[float], decimals: int = 0, suffix: str = "") -> str:
            if value is None or pd.isna(value):
                return "N/A"
            return f"{value:,.{decimals}f}{suffix}"

        # ---- Citywide metrics -------------------------------------------------
        card1 = ui.card(
            ui.h4("Avg daily volume (2024)"),
            ui.p(
                fmt(AVG_DAILY_2024, 0, " vehicles"),
                "Average number of vehicles per day across all boroughs in 2024.",
            ),
        )
        card2 = ui.card(
            ui.h4("Avg daily volume (2025)"),
            ui.p(
                fmt(AVG_DAILY_2025, 0, " vehicles"),
                "Average number of vehicles per day across all boroughs in 2025.",
            ),
        )
        card3 = ui.card(
            ui.h4("Daily volume change (25 vs 24)"),
            ui.p(
                fmt(PCT_CHANGE_DAILY, 1, " %"),
                "Positive values mean higher average daily traffic in 2025 vs 2024.",
            ),
        )
        card4 = ui.card(
            ui.h4("Peak-hours volume change (25 vs 24)"),
            ui.p(
                fmt(PCT_CHANGE_PEAK, 1, " %"),
                "Change in average daily traffic during congestion-pricing peak hours.",
            ),
        )

        # Helper to compute group-specific stats for Manhattan vs outer boroughs
        def group_stats(mask_manhattan: bool):
            # Daily averages
            d = daily_df.copy()
            d["year"] = d["date"].dt.year
            d = d[d["year"].isin([2024, 2025])]
            if mask_manhattan:
                d = d[d["boro"] == "Manhattan"]
            else:
                d = d[d["boro"] != "Manhattan"]

            if d.empty:
                return None, None, None, None

            daily_totals = (
                d.groupby("date", as_index=False)["daily_volume"]
                .sum()
                .rename(columns={"daily_volume": "group_daily_volume"})
            )
            daily_totals["year"] = daily_totals["date"].dt.year

            avg_2024 = daily_totals.loc[
                daily_totals["year"] == 2024, "group_daily_volume"
            ].mean()
            avg_2025 = daily_totals.loc[
                daily_totals["year"] == 2025, "group_daily_volume"
            ].mean()

            pct_change_daily: Optional[float] = None
            if pd.notna(avg_2024) and avg_2024 > 0 and pd.notna(avg_2025):
                pct_change_daily = (
                    float(avg_2025) - float(avg_2024)
                ) / float(avg_2024) * 100.0

            # Peak-hours change using raw_df
            if raw_df.empty:
                pct_change_peak: Optional[float] = None
            else:
                r = raw_df.copy()
                r["date"] = pd.to_datetime(r["date"], errors="coerce")
                r = r.dropna(subset=["date", "hh", "vol", "boro"])
                if mask_manhattan:
                    r = r[r["boro"] == "Manhattan"]
                else:
                    r = r[r["boro"] != "Manhattan"]

                if r.empty:
                    pct_change_peak = None
                else:
                    r["hh"] = pd.to_numeric(r["hh"], errors="coerce")
                    r = r.dropna(subset=["hh"])

                    r["is_weekday"] = r["date"].dt.weekday < 5
                    weekday_peak = r["is_weekday"] & (r["hh"] >= 5) & (r["hh"] < 21)
                    weekend_peak = ~r["is_weekday"] & (r["hh"] >= 9) & (r["hh"] < 21)

                    r_peak = r[weekday_peak | weekend_peak]
                    if r_peak.empty:
                        pct_change_peak = None
                    else:
                        daily_peak = (
                            r_peak.groupby("date", as_index=False)["vol"]
                            .sum()
                            .rename(columns={"vol": "peak_daily_volume"})
                        )
                        daily_peak["year"] = daily_peak["date"].dt.year

                        p24 = daily_peak.loc[
                            daily_peak["year"] == 2024, "peak_daily_volume"
                        ].mean()
                        p25 = daily_peak.loc[
                            daily_peak["year"] == 2025, "peak_daily_volume"
                        ].mean()

                        if pd.notna(p24) and p24 > 0 and pd.notna(p25):
                            pct_change_peak = (
                                float(p25) - float(p24)
                            ) / float(p24) * 100.0
                        else:
                            pct_change_peak = None

            return (
                float(avg_2024) if pd.notna(avg_2024) else None,
                float(avg_2025) if pd.notna(avg_2025) else None,
                pct_change_daily,
                pct_change_peak,
            )

        try:
            (
                man_2024,
                man_2025,
                man_pct_daily,
                man_pct_peak,
            ) = group_stats(mask_manhattan=True)
            (
                outer_2024,
                outer_2025,
                outer_pct_daily,
                outer_pct_peak,
            ) = group_stats(mask_manhattan=False)
        except Exception as exc:  # noqa: BLE001
            # If anything goes wrong computing group stats, fall back to a simple message.
            print(f"Error computing Manhattan/outer-borough stats: {exc}")
            return ui.div(
                ui.layout_columns(
                    card1, card2, card3, card4, col_widths=(3, 3, 3, 3)
                ),
                ui.hr(),
                ui.p(
                    "Unable to compute Manhattan vs outer-borough statistics due "
                    "to an internal error."
                ),
            )

        # ---- Within and outside congestion-pricing zone ----------------------
        manhattan_cards = [
            ui.card(
                ui.h4("Avg daily volume (2024)"),
                ui.p(
                    fmt(man_2024, 0, " vehicles"),
                    "Average vehicles per day within the congestion-pricing zone.",
                ),
            ),
            ui.card(
                ui.h4("Avg daily volume (2025)"),
                ui.p(
                    fmt(man_2025, 0, " vehicles"),
                    "Average vehicles per day within the congestion-pricing zone.",
                ),
            ),
            ui.card(
                ui.h4("Daily volume change (25 vs 24)"),
                ui.p(
                    fmt(man_pct_daily, 1, " %"),
                    "How much daily traffic changed inside the zone after pricing.",
                ),
            ),
            ui.card(
                ui.h4("Peak-hours change (25 vs 24)"),
                ui.p(
                    fmt(man_pct_peak, 1, " %"),
                    "Change in peak-hours traffic inside the congestion-pricing zone.",
                ),
            ),
        ]

        outer_cards = [
            ui.card(
                ui.h4("Avg daily volume (2024)"),
                ui.p(
                    fmt(outer_2024, 0, " vehicles"),
                    "Average vehicles per day outside the congestion-pricing zone.",
                ),
            ),
            ui.card(
                ui.h4("Avg daily volume (2025)"),
                ui.p(
                    fmt(outer_2025, 0, " vehicles"),
                    "Average vehicles per day outside the congestion-pricing zone.",
                ),
            ),
            ui.card(
                ui.h4("Daily volume change (25 vs 24)"),
                ui.p(
                    fmt(outer_pct_daily, 1, " %"),
                    "How much daily traffic changed outside the zone after pricing.",
                ),
            ),
            ui.card(
                ui.h4("Peak-hours change (25 vs 24)"),
                ui.p(
                    fmt(outer_pct_peak, 1, " %"),
                    "Change in peak-hours traffic outside the congestion-pricing zone.",
                ),
            ),
        ]

        return ui.div(
            ui.h3("Citywide overview"),
            ui.layout_columns(card1, card2, card3, card4, col_widths=(3, 3, 3, 3)),
            ui.hr(),
            ui.h3("Within congestion-pricing zone (Manhattan)"),
            ui.layout_columns(*manhattan_cards, col_widths=(3, 3, 3, 3)),
            ui.hr(),
            ui.h3("Outside congestion-pricing zone (outer boroughs)"),
            ui.layout_columns(*outer_cards, col_widths=(3, 3, 3, 3)),
        )


app = App(app_ui, server)

