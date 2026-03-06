"""
Simple zone maps using rough polygons from public sources.
London: Congestion Charge Zone (approximate). Singapore: Central / ERP area (approximate).
"""
from typing import Optional

# Rough London CCZ (central London, ~since 2011 boundary). [lat, lon] order for Leaflet/folium.
# Approximate outline: not cadastral; for context only.
LONDON_CCZ_POLYGON = [
    [51.520, -0.170],  # NW
    [51.520, -0.070],  # NE
    [51.505, -0.070],  # E
    [51.490, -0.050],  # SE
    [51.480, -0.100],  # S
    [51.485, -0.170],  # SW
    [51.520, -0.170],
]

# Rough Singapore central / ERP area (CBD and Orchard area). [lat, lon].
SINGAPORE_CENTRAL_POLYGON = [
    [1.305, 103.830],  # NW
    [1.305, 103.860],  # NE
    [1.275, 103.860],  # E
    [1.265, 103.840],  # SE
    [1.275, 103.820],  # SW
    [1.305, 103.830],
]


def get_zone_map_html(city: str) -> str:
    """
    Return HTML string for a Folium map with the zone polygon.
    city in ("london", "singapore").
    """
    try:
        import folium
    except ImportError:
        return "<p>Map unavailable (install folium: pip install folium).</p>"

    if city == "london":
        center = [51.505, -0.12]
        zoom = 14
        polygon = LONDON_CCZ_POLYGON
        title = "London Congestion Charge Zone (approximate)"
    elif city == "singapore":
        center = [1.285, 103.845]
        zoom = 13
        polygon = SINGAPORE_CENTRAL_POLYGON
        title = "Singapore central / ERP area (approximate)"
    else:
        return "<p>No map for this city.</p>"

    m = folium.Map(location=center, zoom_start=zoom, tiles="OpenStreetMap")
    folium.Polygon(
        locations=polygon,
        color="blue",
        weight=2,
        fill=True,
        fill_opacity=0.15,
        popup=title,
    ).add_to(m)
    return m._repr_html_()
