# cpportal_transport

## Congestion Pricing Dashboard

A Shiny (Python) app for London and Singapore congestion pricing impact: vehicle entries, baseline vs comparison, policy reference dates, and AI-generated recommendations.

**Run the dashboard:**
```bash
shiny run congestion_dashboard_app.py
```

- **Data**: Reads from Supabase (Postgres). Ensure `.env` has `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` (or `SUPABASE_DB_URL`). Run ingestion first: `python ingest_london_ccz.py`, `python ingest_singapore_annual.py`.
- **Policy recommendations**: Optional. Uses local **Ollama** (no API key). Set `OLLAMA_HOST` (default `http://localhost:11434`) and `OLLAMA_MODEL` (e.g. `smollm2:1.7b`) in `.env`. Start Ollama with `ollama serve` and pull a model (e.g. `ollama pull smollm2:1.7b`).
- **Map**: Rough zone polygons (London CCZ, Singapore central). Requires `folium`: `pip install folium`.
- **NYC**: Shown in the city selector but greyed out (coming later).