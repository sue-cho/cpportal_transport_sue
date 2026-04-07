"""
map_utils.py — city centroid map utilities for congestion pricing dashboard.
Replaces Bar Harbor PathLayer with ScatterplotLayer on city lat/lon centroids.
"""

from __future__ import annotations

from typing import Optional
import pandas as pd
import pydeck as pdk

# City center fallbacks if lat/lon missing from DB
CITY_DEFAULTS = {
    "SGP": (1.3521, 103.8198, "Singapore"),
    "LON": (51.5074, -0.1278, "London"),
    "STO": (59.3293, 18.0686, "Stockholm"),
}

WORLD_CENTER = (20.0, 0.0, 1.5)


def _clean_str(value: object, default: str = "") -> str:
    """Return a plain string; avoid pandas.NA/NaN leaking into PyDeck JSON."""
    if value is None or pd.isna(value):
        return default
    return str(value)


def _clean_float(value: object, default: float) -> float:
    """Return a plain float; fall back when value is missing/invalid."""
    if value is None or pd.isna(value):
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def build_city_map(
    cities_df: pd.DataFrame,
    obs_df: Optional[pd.DataFrame] = None,
) -> pdk.Deck:
    """
    Build a PyDeck ScatterplotLayer showing city centroids.
    If obs_df is provided, circles are sized by mean vehicle count
    and colored green (untreated) / coral (treated) based on most
    recent policy period for that city.
    """
    if cities_df is None or cities_df.empty:
        cities_df = pd.DataFrame([
            {"city_code": k, "city_name": v[2], "lat": v[0], "lon": v[1]}
            for k, v in CITY_DEFAULTS.items()
        ])

    rows = []
    for _, city in cities_df.iterrows():
        raw_code = city.get("city_code", "")
        code = _clean_str(raw_code, "")
        defaults = CITY_DEFAULTS.get(code, WORLD_CENTER[:2])
        lat = _clean_float(city.get("lat"), defaults[0])
        lon = _clean_float(city.get("lon"), defaults[1])
        name = _clean_str(city.get("city_name"), code or "Unknown city")

        mean_val = None
        treated = None
        pct_change = None

        if obs_df is not None and not obs_df.empty and "city_code" in obs_df.columns:
            city_obs = obs_df[obs_df["city_code"] == code]
            if not city_obs.empty:
                mean_val = city_obs["value"].mean()
                # Most recent period determines treated status
                if "treated" in city_obs.columns:
                    latest = city_obs.sort_values("obs_year").iloc[-1]
                    treated = bool(latest.get("treated", False))
                # Compute pct change if both treated and untreated exist
                pre = city_obs[~city_obs["treated"]]["value"].mean() if "treated" in city_obs.columns else None
                post = city_obs[city_obs["treated"]]["value"].mean() if "treated" in city_obs.columns else None
                if (
                    pre is not None
                    and post is not None
                    and pd.notna(pre)
                    and pd.notna(post)
                    and pre != 0
                ):
                    pct_change = round((post - pre) / pre * 100, 1)

        # Color: teal = has pricing, gray = no policy, blue = unknown
        if treated is True:
            color = [29, 158, 117]   # teal — policy active
        elif treated is False:
            color = [136, 135, 128]  # gray — pre-policy
        else:
            color = [55, 138, 221]   # blue — unknown

        # Radius scaled loosely by vehicle count (min 80k, max 300k)
        radius = 40000
        if mean_val is not None:
            radius = int(max(30000, min(120000, mean_val / 4)))

        pct_str = (
            f"{pct_change:+.1f}%"
            if pct_change is not None and pd.notna(pct_change)
            else "N/A"
        )
        val_str = f"{int(mean_val):,}" if mean_val is not None else "N/A"

        rows.append({
            "city_name": name,
            "city_code": code,
            "lat": lat,
            "lon": lon,
            "color": color,
            "radius": radius,
            "tooltip_value": val_str,
            "tooltip_pct": pct_str,
            "tooltip_status": "Policy active" if treated else ("Pre-policy" if treated is False else "Unknown"),
        })

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=rows,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        radius_min_pixels=8,
        radius_max_pixels=60,
        pickable=True,
        opacity=0.75,
        stroked=True,
        get_line_color=[255, 255, 255],
        line_width_min_pixels=2,
    )

    # Center view on mean of all city coords
    # Ensure plain Python floats (not numpy scalars) for PyDeck JSON serialization.
    if rows:
        center_lat = float(sum(float(r["lat"]) for r in rows) / len(rows))
        center_lon = float(sum(float(r["lon"]) for r in rows) / len(rows))
    else:
        center_lat = float(WORLD_CENTER[0])
        center_lon = float(WORLD_CENTER[1])

    view = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=2.5,
        pitch=0,
        bearing=0,
    )

    tooltip = {
        "html": (
            "<b>{city_name}</b><br/>"
            "Avg vehicles: {tooltip_value}<br/>"
            "Change vs pre-policy: {tooltip_pct}<br/>"
            "Status: {tooltip_status}"
        ),
        "style": {
            "backgroundColor": "white",
            "color": "#1e293b",
            "border": "1px solid #e2e8f0",
            "borderRadius": "8px",
            "padding": "8px",
            "fontSize": "13px",
        },
    }

    return pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip=tooltip,
        map_style="light",
        map_provider="carto",
    )
