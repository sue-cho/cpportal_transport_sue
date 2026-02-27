## NYC Automated Traffic Volume Counts API (7ym2-wayt)

This project uses the **NYC Open Data** dataset **“Automated Traffic Volume Counts”** published by NYC DOT:

- Dataset page: `https://data.cityofnewyork.us/Transportation/Automated-Traffic-Volume-Counts/7ym2-wayt`
- JSON API endpoint: `https://data.cityofnewyork.us/resource/7ym2-wayt.json`
- API docs: `https://dev.socrata.com/foundry/data.cityofnewyork.us/7ym2-wayt`

The dataset contains traffic sample volume counts collected using Automated Traffic Recorders (ATR) at bridge crossings and roadways. Each row is typically a 15‑minute vehicle count at a specific location and time.

### Important fields used

The app mainly relies on:

- **`boro`**: Borough name (e.g., `Manhattan`, `Queens`).
- **`yr`**: Year (e.g., `2024`).
- **`m`**: Month number (`1`–`12`).
- **`d`**: Day of month (`1`–`31`).
- **`hh`**: Hour of day (`0`–`23`).
- **`mm`**: Minute of hour (`0, 15, 30, 45`).
- **`vol`**: Vehicle count during that 15‑minute interval.
- **`segmentid`**: Roadway/segment identifier.
- **`street`, `fromst`, `tost`**: Location description.
- **`direction`**: Travel direction.

In `nyc_traffic_data.py` these are converted into:

- A numeric **`vol`** column.
- A proper pandas **`date`** column built from `yr`, `m`, `d` for daily/monthly aggregation.

### How `nyc_traffic_data.py` queries the API

`nyc_traffic_data.py` provides two key functions:

- **`fetch_traffic_data(start_year=2024, end_year=2025, app_token=None, limit=500_000)`**  
  - Issues a GET request to `https://data.cityofnewyork.us/resource/7ym2-wayt.json`.
  - Uses **SoQL** parameters:
    - `$select`: a subset of columns (`requestid, boro, yr, m, d, hh, mm, vol, segmentid, street, fromst, tost, direction`).
    - `$where`: e.g. `yr >= '2024' AND yr <= '2025'`.
    - `$limit`: default `500000` rows.
  - Adds an `X-App-Token` header using your configured app token.
  - Returns a pandas `DataFrame` with raw records plus a `date` column.

- **`build_aggregates(df, pricing_start="2025-01-01")`**  
  - Computes **daily** total volume per borough.
  - Labels days as **before** vs **on/after** the congestion-pricing reference date.
  - Computes **monthly** total volume per borough and period.

These aggregates are then consumed by the Shiny app (`nyc_traffic_app.py`) to produce:

- Daily Plotly line chart with a reference line at **2025‑01‑01**.
- Monthly grouped bar chart comparing **before vs after**.
- Summary statistics on daily volumes before vs after.

### Authentication: getting and configuring an app token

NYC Open Data (Socrata) strongly recommends using an **Application Token** for API access.

1. **Create an Application Token**
   - Go to `https://data.cityofnewyork.us` and sign in.
   - Open your **profile → Developer / API** section.
   - Create a new **Application Token** for the domain `data.cityofnewyork.us`.
   - Copy the generated token string (keep it secret).

2. **Add the token to your `.env`**

Create or edit `.env` in the project root:

```env
NYC_OPENDATA_APP_TOKEN=your_real_socrata_app_token_here
```

Optionally you can also set:

```env
NYC_OPENDATA_API_KEY=your_real_socrata_app_token_here
```

`nyc_traffic_data.py` will use `NYC_OPENDATA_API_KEY` if set; otherwise it falls back to `NYC_OPENDATA_APP_TOKEN`.

3. **How the token is used in `nyc_traffic_data.py`**

- At the start of `fetch_traffic_data`, the module calls **`load_dotenv()`** (from `python-dotenv`) so `.env` is read automatically.
- It resolves the token as:

```python
app_token = (
    os.getenv("NYC_OPENDATA_API_KEY")
    or os.getenv("NYC_OPENDATA_APP_TOKEN")
)
```

- If a token is present, it is sent on every request as:

```http
X-App-Token: <your_token_here>
```

If the request fails (network error, invalid token, etc.), `fetch_traffic_data` prints an error message and returns an **empty DataFrame**, allowing the UI to show “no data” instead of crashing.

### Python dependencies

To use `nyc_traffic_data.py` and the Shiny app end‑to‑end you need:

```bash
pip install pandas requests python-dotenv shiny shinywidgets plotly anywidget
```

If you are using conda, you can also install `pandas` and `requests` via conda and keep `shiny`, `shinywidgets`, `python-dotenv`, `plotly`, and `anywidget` from pip.

### Running the Shiny app with the API

1. Ensure `.env` in the project root contains a valid `NYC_OPENDATA_APP_TOKEN` (or `NYC_OPENDATA_API_KEY`).
2. From the project root, run:

```bash
shiny run --reload nyc_traffic_app.py
```

On startup, the app will:

- Load your `.env` and read the NYC Open Data token.
- Call `fetch_traffic_data` for the desired year range.
- Aggregate daily and monthly volumes via `build_aggregates`.
- Display interactive visualizations of traffic volume trends before and after congestion pricing in 2025.

