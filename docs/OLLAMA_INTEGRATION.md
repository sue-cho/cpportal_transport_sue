# Dashboard implementation notes

## Plan review – suggested improvements

- **Loading and empty states**: Show a spinner or “Loading…” when switching cities or when Supabase queries run; show a clear “No data for this period” (or “Select a baseline period”) when filters return no rows. Avoid blank panels.
- **Caching**: Cache Supabase results per (city, baseline range, comparison range) in the Shiny session so changing only the reference date or the map doesn’t re-query the same series. Invalidate when city or date ranges change.
- **Ollama timeout and UX**: Use a 30–45 s timeout for the recommendations request and show “Generating…” while waiting. If the request times out, show “Recommendations took too long; try again” instead of a generic error.
- **Metric labels and tooltips**: Label London metrics as “CCZ vehicle entries (charging hours)” and Singapore as “Avg daily traffic entering city (7:30–19:00 weekdays)” so users know what they’re looking at. Add a small (i) or tooltip on KPI cards with a one-line definition.
- **Baseline vs comparison labels**: Always show the exact date ranges used for “Baseline” and “Comparison (most current)” above the chart or in the KPI card subtitle (e.g. “Baseline: Jan 2012 – Dec 2013; Comparison: Jan 2023 – Dec 2024”) so the “% vs baseline” is interpretable.
- **London metric toggle**: In the London view, include a toggle or dropdown for “Camera captures” vs “Confirmed vehicles” and persist the choice in the session so the chart and KPIs use the same metric.
- **Error boundary for map**: If Leaflet or the polygon fails to load (e.g. missing GeoJSON), show a fallback message and the table of latest snapshot data instead of a broken map.
- **Accessibility**: Use semantic headings (h2 for “Time since implementation”, “KPI”, “Policy recommendations”, “Map”) and ensure city selector buttons and date inputs are keyboard-focusable and have clear labels for screen readers.
- **Public transit placeholder**: Either show a fourth card “Public transit: Data not available” for consistency with the Figma, or omit it and keep three cards (vehicle entries, traffic volume, revenue placeholder) so the layout doesn’t look empty.
- **README**: Add a “Congestion dashboard” section to the main README: how to run the new app (`shiny run congestion_dashboard_app.py`), that Ollama is optional (recommendations only), and that `.env` should have DB vars and optionally OLLAMA_HOST / OLLAMA_MODEL.

---

## Map: zone polygons

Use **rough polygons from public sources** for the simple map:

- **London Congestion Charge Zone**: Approximate boundary (e.g. from OpenStreetMap, TfL open data, or a published approximate polygon). Draw as a Leaflet polygon overlay on the base map.
- **Singapore central / ERP area**: Approximate boundary from public sources (e.g. LTA, OpenStreetMap, or government open data). Same approach: Leaflet polygon.

No need for precise cadastral boundaries; rough outlines are sufficient for context. Snapshot data (TfL corridors, LTA segments) can be overlaid as points/lines if coordinates are available, or shown in a table below the map.

---

# Ollama integration for policy recommendations

The dashboard will call a local Ollama instance for AI-generated policy recommendations, following the same pattern as the course example.

## Pattern (from `02_ollama.py`)

- **Endpoint**: `http://localhost:11434/api/generate` (no API key for local Ollama).
- **Request**: POST with JSON body:
  ```json
  {
    "model": "smollm2:1.7b",
    "prompt": "<summary of metrics + ask for 2-3 recommendations>",
    "stream": false
  }
  ```
- **Response**: JSON with `response` (generated text) and optionally `error`.
- **Implementation**: `requests.post(url, json=body, timeout=60)`, then `response.json()["response"]`; handle `"error"` in response.

## Dashboard usage

- **Recommendations module** will:
  - Use `OLLAMA_HOST` from `.env` (default `http://localhost:11434`) and optional `OLLAMA_MODEL` (default e.g. `smollm2:1.7b`).
  - Build a prompt with: city, reference date, baseline vs comparison summary (vehicle entries, % change, revenue N/A).
  - POST to `{OLLAMA_HOST}/api/generate` with `stream: False`.
  - Parse the reply and show 2–3 bullets in the Policy recommendations card.
- **OLLAMA_API_KEY** in `.env` is optional (reserved for future remote Ollama or proxy that requires a key); local Ollama does not use it.
- If Ollama is unreachable or returns an error, show: “Recommendations unavailable (is Ollama running? e.g. `ollama serve`).”
