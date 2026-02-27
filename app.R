#
# NYC Traffic Before/After Congestion Pricing (Jan 1, 2025)
#
# Shiny dashboard that:
# - Pulls NYC traffic data for 2024 and 2025
# - Filters to congestion-pricing-relevant peak hours (weekdays, 5 AM–9 PM)
# - Aggregates average peak-hour traffic volume
# - Compares 2024 (pre-CP baseline) vs 2025 (post-CP)
# - Displays spatial heat map, time series panel, and KPI summary
#
# Major sections:
# - API query logic (with SoQL filters and pagination)
# - Data cleaning and peak-hour filtering
# - Spatial aggregation and percent change computation
# - Time series construction (daily/weekly)
# - Shiny UI (fluidPage layout, sidebar filters)
# - Server logic (reactive cache, KPIs, map, time series)

# ---- Package imports ----
library(shiny)
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(leaflet)
library(sf)
library(scales)
library(ggplot2)
library(tidyr)

# ---- Global configuration ----
BASE_URL <- "https://data.cityofnewyork.us/resource/7ym2-wayt.json"
TARGET_YEARS <- c(2024L, 2025L)
CP_START_DATE <- as.Date("2025-01-01")

# Approximate latitude for the 60th Street boundary in Manhattan.
# Assumption: locations in Manhattan with latitude below this threshold
# are considered inside the congestion pricing (CP) zone.
CP_LAT_THRESHOLD <- 40.77

# ---- Helper: safe percent change (handles divide-by-zero) ----
safe_percent_change <- function(new, old) {
  ifelse(is.na(old) | old == 0, NA_real_, (new - old) / old * 100)
}

# ---- Helper: API pagination + SoQL filters (year, hour, weekday as far as schema allows) ----
fetch_year_data <- function(year, app_token, page_limit = 50000) {
  offset <- 0L
  all_pages <- list()

  repeat {
    # SoQL filter:
    # - Restrict to specific year
    # - Restrict to peak hours (5 AM–9 PM)
    # Weekday filter is fully enforced later in R because weekday fields are not
    # directly present in the dataset schema sample.
    where_clause <- sprintf("yr = '%d' AND hh >= 5 AND hh < 21", year)

    resp <- GET(
      BASE_URL,
      add_headers("X-App-Token" = app_token),
      query = list(
        "$where" = where_clause,
        "$limit" = page_limit,
        "$offset" = offset
      ),
      timeout(60)
    )

    # Basic HTTP error handling
    stop_for_status(resp)

    txt <- content(resp, as = "text", encoding = "UTF-8")
    if (identical(txt, "") || is.null(txt)) break

    dat <- fromJSON(txt, flatten = TRUE)

    # ---- Pagination break condition ----
    if (is.null(dat) || length(dat) == 0) break

    dat <- as_tibble(dat)
    all_pages[[length(all_pages) + 1L]] <- dat

    # If last page is smaller than limit, no more pages to fetch
    if (nrow(dat) < page_limit) break

    offset <- offset + page_limit
  }

  if (length(all_pages) == 0) {
    return(NULL)
  }

  bind_rows(all_pages)
}

# ---- Helper: clean and enrich raw API data ----
clean_traffic_data <- function(raw_df) {
  if (is.null(raw_df) || nrow(raw_df) == 0) {
    return(data.frame())
  }

  df <- raw_df %>%
    # Ensure numeric/integer fields for time and volume
    mutate(
      yr = suppressWarnings(as.integer(yr)),
      m = suppressWarnings(as.integer(m)),
      d = suppressWarnings(as.integer(d)),
      hh = suppressWarnings(as.integer(hh)),
      mm = suppressWarnings(as.integer(mm)),
      vol = suppressWarnings(as.numeric(vol))
    ) %>%
    # Drop rows without essential time/volume info
    filter(!is.na(yr), !is.na(m), !is.na(d), !is.na(hh), !is.na(mm), !is.na(vol)) %>%
    # ---- Peak-hour timestamp construction using lubridate ----
    mutate(
      timestamp = make_datetime(
        year = yr,
        month = m,
        day = d,
        hour = hh,
        min = mm,
        tz = "America/New_York"
      ),
      date = as.Date(timestamp),
      year = year(timestamp),
      month = month(timestamp),
      hour = hour(timestamp),
      weekday = wday(timestamp, label = TRUE, abbr = TRUE),
      is_weekday = !weekday %in% c("Sat", "Sun")
    ) %>%
    # ---- Final peak-hour and weekday filtering (safety filter) ----
    filter(
      year %in% TARGET_YEARS,
      is_weekday,
      hour >= 5,
      hour < 21
    )

  # ---- Geometry: derive longitude/latitude from WKT (if available) ----
  if ("wktgeom" %in% names(df)) {
    seg_geom <- df %>%
      distinct(segmentid, wktgeom) %>%
      filter(!is.na(wktgeom))

    if (nrow(seg_geom) > 0) {
      sf_geom <- st_as_sfc(seg_geom$wktgeom, crs = 2263)
      sf_geom <- st_transform(sf_geom, 4326)
      coords <- st_coordinates(sf_geom)
      seg_geom$lon <- coords[, 1]
      seg_geom$lat <- coords[, 2]

      df <- df %>%
        left_join(seg_geom, by = c("segmentid", "wktgeom"))
    } else {
      df$lon <- NA_real_
      df$lat <- NA_real_
    }
  } else {
    df$lon <- NA_real_
    df$lat <- NA_real_
  }

  # ---- Spatial classification: CP Zone vs Manhattan Outside vs Outer Boroughs ----
  # CP Zone = Manhattan AND latitude below ~60th Street (CP_LAT_THRESHOLD).
  # Manhattan Outside Zone = remainder of Manhattan (lat >= threshold or missing).
  # Outer Boroughs = all non-Manhattan boroughs.
  df %>%
    mutate(
      boro_upper = toupper(boro),
      zone_category = case_when(
        boro_upper != "MANHATTAN" ~ "Outer Boroughs",
        !is.na(lat) & lat < CP_LAT_THRESHOLD ~ "CP Zone",
        TRUE ~ "Manhattan Outside Zone"
      ),
      zone_category = factor(
        zone_category,
        levels = c("CP Zone", "Manhattan Outside Zone", "Outer Boroughs")
      )
    )
}

# ---- Helper: zone-level aggregation for map and KPIs ----
build_zone_aggregation <- function(df) {
  if (nrow(df) == 0) {
    return(data.frame())
  }

  # ---- Step 1: aggregate to location (segment) level by zone and year ----
  loc_year <- df %>%
    group_by(segmentid, zone_category, year) %>%
    summarise(
      avg_volume = mean(vol, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(year = paste0("avg_", year)) %>%
    tidyr::pivot_wider(
      names_from = year,
      values_from = avg_volume
    )

  # Ensure both baseline and post-CP columns exist
  if (!"avg_2024" %in% names(loc_year)) loc_year$avg_2024 <- NA_real_
  if (!"avg_2025" %in% names(loc_year)) loc_year$avg_2025 <- NA_real_

  # ---- Step 2: filter out locations with very low 2024 baseline ----
  # This prevents unstable percent changes for near-zero baseline volumes.
  loc_year <- loc_year %>%
    filter(!is.na(avg_2024), avg_2024 >= 10)

  if (nrow(loc_year) == 0) {
    return(data.frame())
  }

  # ---- Step 3: zone-level aggregation based on filtered locations ----
  zone_agg <- loc_year %>%
    group_by(zone_category) %>%
    summarise(
      avg_2024 = mean(avg_2024, na.rm = TRUE),
      avg_2025 = mean(avg_2025, na.rm = TRUE),
      .groups = "drop"
    )

  # ---- Step 4: percent change and absolute difference with capping ----
  zone_agg %>%
    mutate(
      abs_diff = avg_2025 - avg_2024,
      pct_change_raw = safe_percent_change(avg_2025, avg_2024),
      # Cap extreme percent changes to ±20% for stable visualization
      pct_change = pmax(pmin(pct_change_raw, 20), -20)
    )
}

# ---- Helper: time series aggregation (daily or weekly) by congestion-pricing zone ----
build_time_series <- function(df, agg_level = c("day", "week")) {
  agg_level <- match.arg(agg_level)

  if (nrow(df) == 0) {
    return(data.frame())
  }

  start_date <- as.Date("2024-01-01")
  end_date <- max(df$date, na.rm = TRUE)

  # ---- Daily time series construction ----
  if (agg_level == "day") {
    ts <- df %>%
      group_by(date, zone_category) %>%
      summarise(
        avg_volume = mean(vol, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      complete(
        zone_category,
        date = seq.Date(start_date, end_date, by = "day")
      ) %>%
      arrange(date, zone_category) %>%
      rename(ts_date = date)
  } else {
    # ---- Weekly time series construction ----
    df_week <- df %>%
      mutate(week_start = floor_date(date, unit = "week", week_start = 1))

    start_week <- floor_date(start_date, unit = "week", week_start = 1)
    end_week <- floor_date(end_date, unit = "week", week_start = 1)

    ts <- df_week %>%
      group_by(week_start, zone_category) %>%
      summarise(
        avg_volume = mean(vol, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      complete(
        zone_category,
        week_start = seq.Date(start_week, end_week, by = "week")
      ) %>%
      arrange(week_start, zone_category) %>%
      rename(ts_date = week_start)
  }

  ts
}

# ---- UI layout (fluidPage) ----
ui <- fluidPage(
  # ---- Global styles for KPI cards, layout, and loading spinner ----
  tags$head(
    tags$style(HTML("
      .kpi-row {
        margin-bottom: 20px;
      }
      .kpi-box {
        background-color: #f7f7f7;
        border-radius: 6px;
        padding: 12px 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        margin-bottom: 10px;
      }
      .kpi-title {
        font-size: 12px;
        text-transform: uppercase;
        color: #666666;
        margin-bottom: 4px;
      }
      .kpi-value {
        font-size: 20px;
        font-weight: 600;
        color: #333333;
      }
      .kpi-subtext {
        font-size: 11px;
        color: #999999;
      }
      .loading-overlay {
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 25px;
        color: #555555;
      }
      .spinner {
        border: 4px solid #f3f3f3;
        border-top: 4px solid #007bff;
        border-radius: 50%;
        width: 22px;
        height: 22px;
        animation: spin 1s linear infinite;
        margin-right: 10px;
      }
      @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
      }
    "))
  ),

  titlePanel("NYC Traffic Before/After Congestion Pricing (Peak Hours)"),

  sidebarLayout(
    sidebarPanel(
      width = 3,

      # ---- Sidebar: Borough filter (All + specific boroughs) ----
      selectInput(
        "borough",
        "Borough",
        choices = c("All"),
        selected = "All"
      ),

      # ---- Sidebar: Metric selector for map visualization ----
      selectInput(
        "metric",
        "Map Metric",
        choices = c(
          "2024 Average" = "avg_2024",
          "2025 Average" = "avg_2025",
          "Percent Change" = "pct_change",
          "Absolute Difference" = "abs_diff"
        ),
        selected = "pct_change"
      ),

      # ---- Sidebar: Time aggregation toggle (daily vs weekly) ----
      radioButtons(
        "agg_level",
        "Time Series Aggregation",
        choices = c("Daily view" = "day", "Weekly view" = "week"),
        selected = "day"
      ),

      helpText("Analysis restricted to weekdays, 5 AM–9 PM, for years 2024 and 2025.")
    ),

    mainPanel(
      width = 9,

      # ---- Loading indicator while API is fetching (reactiveVal cache being filled) ----
      uiOutput("data_loading_indicator"),

      # ---- Section 1: KPI summary metrics (top row) ----
      fluidRow(
        class = "kpi-row",
        column(
          3,
          div(
            class = "kpi-box",
            div(class = "kpi-title", "Avg 2024 Peak Traffic"),
            div(textOutput("kpi_avg_2024"), class = "kpi-value"),
            div(class = "kpi-subtext", "Weekdays, 5 AM–9 PM")
          )
        ),
        column(
          3,
          div(
            class = "kpi-box",
            div(class = "kpi-title", "Avg 2025 Peak Traffic"),
            div(textOutput("kpi_avg_2025"), class = "kpi-value"),
            div(class = "kpi-subtext", "Weekdays, 5 AM–9 PM")
          )
        ),
        column(
          3,
          div(
            class = "kpi-box",
            div(class = "kpi-title", "Overall Percent Change"),
            div(textOutput("kpi_pct_change"), class = "kpi-value"),
            div(class = "kpi-subtext", "2025 vs 2024 (avg volume)")
          )
        ),
        column(
          3,
          div(
            class = "kpi-box",
            div(class = "kpi-title", "Total Vehicle Change"),
            div(textOutput("kpi_total_change"), class = "kpi-value"),
            div(class = "kpi-subtext", "Sum of peak-hour counts")
          )
        )
      ),

      # ---- Section 2: Interactive Leaflet map ----
      h4("Spatial Distribution of Peak-Hour Traffic Change"),
      leafletOutput("traffic_map", height = 500),

      br(),

      # ---- Section 3: Before/After time series panel ----
      h4("Before/After Time Series (Peak-Hour Average Volume)"),
      plotOutput("traffic_time_series", height = 350)
    )
  )
)

# ---- Server logic ----
server <- function(input, output, session) {
  # ---- Reactive cache: raw API data (loaded once per session) ----
  traffic_raw <- reactiveVal(NULL)

  # ---- Initial data load with progress and spinner (no re-query on UI changes) ----
  observe({
    if (!is.null(traffic_raw())) {
      return()
    }

    # Use Socrata App Token from NYC_OPENDATA_APP_TOKEN (configured in .env or environment)
    app_token <- Sys.getenv("NYC_OPENDATA_APP_TOKEN")

    if (identical(app_token, "")) {
      showNotification(
        "NYC_OPENDATA_APP_TOKEN is not set. Please define this environment variable (e.g. in your .env file) with your Socrata App Token.",
        type = "error",
        duration = NULL
      )
      traffic_raw(data.frame())
      return()
    }

    withProgress(message = "Fetching NYC traffic data...", value = 0, {
      all_years <- list()
      n_years <- length(TARGET_YEARS)

      for (i in seq_along(TARGET_YEARS)) {
        yr <- TARGET_YEARS[i]
        incProgress((i - 1) / n_years, detail = paste("Loading year", yr, "..."))

        dat <- fetch_year_data(yr, app_token = app_token)

        if (!is.null(dat) && nrow(dat) > 0) {
          dat$yr <- yr
          all_years[[length(all_years) + 1L]] <- dat
        }
      }

      if (length(all_years) == 0) {
        showNotification(
          "No traffic data returned for 2024/2025 and specified filters.",
          type = "warning",
          duration = NULL
        )
        traffic_raw(data.frame())
      } else {
        traffic_raw(bind_rows(all_years))
      }

      incProgress(1, detail = "Data load complete.")
    })
  })

  # ---- Data loading spinner UI (only while cache is empty) ----
  output$data_loading_indicator <- renderUI({
    if (is.null(traffic_raw())) {
      div(
        class = "loading-overlay",
        div(class = "spinner"),
        div("Loading NYC traffic data...")
      )
    } else {
      NULL
    }
  })

  # ---- Cleaned and enriched data (peak-hour, weekdays, 2024/2025) ----
  traffic_clean <- reactive({
    req(!is.null(traffic_raw()))
    clean_traffic_data(traffic_raw())
  })

  # ---- Diagnostics: location coverage by year and overlap ----
  observeEvent(traffic_clean(), {
    df <- traffic_clean()
    if (nrow(df) == 0) {
      cat("[Diagnostics] No traffic data available after cleaning.\n")
      return()
    }

    seg_2024 <- unique(df$segmentid[df$year == 2024])
    seg_2025 <- unique(df$segmentid[df$year == 2025])

    overlap_both <- intersect(seg_2024, seg_2025)
    only_2024 <- setdiff(seg_2024, seg_2025)
    only_2025 <- setdiff(seg_2025, seg_2024)

    cat("\n[Diagnostics] Location coverage by year (segmentid):\n")
    cat("  2024 unique locations:", length(seg_2024), "\n")
    cat("  2025 unique locations:", length(seg_2025), "\n")
    cat("  Overlap (locations in both years):", length(overlap_both), "\n\n")

    summary_tbl <- tibble(
      location_group = c("Both years", "Only 2024", "Only 2025"),
      n_locations = c(length(overlap_both), length(only_2024), length(only_2025))
    )

    cat("[Diagnostics] Summary of location overlap:\n")
    print(summary_tbl)
    cat("\n")
  })

  # ---- Update borough selector options based on available data ----
  observeEvent(traffic_clean(), {
    df <- traffic_clean()
    boro_choices <- sort(unique(df$boro))
    boro_choices <- boro_choices[!is.na(boro_choices)]
    updateSelectInput(
      session,
      "borough",
      choices = c("All", boro_choices),
      selected = "All"
    )
  })

  # ---- Borough-filtered dataset (used by KPIs, map, and time series) ----
  traffic_filtered <- reactive({
    df <- traffic_clean()
    if (nrow(df) == 0) {
      return(df)
    }
    if (!is.null(input$borough) && input$borough != "All") {
      df <- df %>% filter(boro == input$borough)
    }
    df
  })

  # ---- KPI aggregation logic ----
  kpi_summary <- reactive({
    df <- traffic_filtered()
    if (nrow(df) == 0) {
      return(list(
        avg2024 = NA_real_,
        avg2025 = NA_real_,
        pct_change = NA_real_,
        total_change = NA_real_
      ))
    }

    summary <- df %>%
      group_by(year) %>%
      summarise(
        avg_volume = mean(vol, na.rm = TRUE),
        total_volume = sum(vol, na.rm = TRUE),
        .groups = "drop"
      )

    avg2024 <- summary$avg_volume[summary$year == 2024]
    avg2025 <- summary$avg_volume[summary$year == 2025]
    total2024 <- summary$total_volume[summary$year == 2024]
    total2025 <- summary$total_volume[summary$year == 2025]

    if (length(avg2024) == 0) avg2024 <- NA_real_
    if (length(avg2025) == 0) avg2025 <- NA_real_
    if (length(total2024) == 0) total2024 <- NA_real_
    if (length(total2025) == 0) total2025 <- NA_real_

    list(
      avg2024 = avg2024,
      avg2025 = avg2025,
      pct_change = safe_percent_change(avg2025, avg2024),
      total_change = total2025 - total2024
    )
  })

  # ---- KPI outputs (formatted text) ----
  output$kpi_avg_2024 <- renderText({
    s <- kpi_summary()
    if (is.na(s$avg2024)) return("N/A")
    comma(s$avg2024, accuracy = 0.1)
  })

  output$kpi_avg_2025 <- renderText({
    s <- kpi_summary()
    if (is.na(s$avg2025)) return("N/A")
    comma(s$avg2025, accuracy = 0.1)
  })

  output$kpi_pct_change <- renderText({
    s <- kpi_summary()
    if (is.na(s$pct_change)) return("N/A")
    paste0(number(s$pct_change, accuracy = 0.1), "%")
  })

  output$kpi_total_change <- renderText({
    s <- kpi_summary()
    if (is.na(s$total_change)) return("N/A")
    sign_label <- ifelse(s$total_change > 0, "+", "")
    paste0(sign_label, comma(s$total_change, accuracy = 1))
  })

  # ---- Zone-level aggregation reactive for map ----
  zone_agg <- reactive({
    df <- traffic_filtered()
    build_zone_aggregation(df)
  })

  # Spatial data with per-location geometry and zone-level stats
  spatial_data <- reactive({
    df <- traffic_filtered()
    zagg <- zone_agg()

    if (nrow(df) == 0 || nrow(zagg) == 0) {
      return(NULL)
    }

    df %>%
      filter(!is.na(lon), !is.na(lat)) %>%
      left_join(
        zagg %>% select(zone_category, avg_2024, avg_2025, abs_diff, pct_change),
        by = "zone_category"
      )
  })

  # ---- Interactive Leaflet map rendering (points colored by zone-level percent change) ----
  output$traffic_map <- renderLeaflet({
    df <- spatial_data()

    if (is.null(df) || nrow(df) == 0) {
      return(
        leaflet() %>%
          addTiles() %>%
          addPopups(
            lng = 0,
            lat = 0,
            popup = "No spatial data available for the selected filters."
          )
      )
    }

    # Metric selector still controls marker sizing; color encodes percent change (capped)
    metric_col <- input$metric
    if (is.null(metric_col) || !metric_col %in% c("avg_2024", "avg_2025", "pct_change", "abs_diff")) {
      metric_col <- "pct_change"
    }

    df <- df %>%
      mutate(
        metric_value = .data[[metric_col]],
        size_value = ifelse(is.na(metric_value), NA_real_, abs(metric_value))
      )

    # Symmetric color scale centered at 0 for zone-level percent change (capped to ±20%)
    pal <- colorNumeric(
      palette = colorRampPalette(c("blue", "white", "red"))(100),
      domain = c(-20, 20)
    )

    # Marker radius scaled by magnitude of selected metric
    size_range <- range(df$size_value, na.rm = TRUE)
    if (!is.finite(size_range[1]) || !is.finite(size_range[2]) || diff(size_range) == 0) {
      radius_vals <- rep(6, nrow(df))
    } else {
      radius_vals <- rescale(df$size_value, to = c(4, 14), from = size_range)
    }

    leaflet(df) %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      addCircleMarkers(
        lng = ~lon,
        lat = ~lat,
        radius = radius_vals,
        color = ~pal(pct_change),
        fillOpacity = 0.8,
        weight = 1,
        popup = ~sprintf(
          "<strong>Zone:</strong> %s<br/><strong>Borough:</strong> %s<br/>2024 Avg: %s<br/>2025 Avg: %s<br/>Percent Change (capped): %s%%",
          as.character(zone_category),
          ifelse(is.na(boro), "Unknown", boro),
          ifelse(is.na(avg_2024), "N/A", comma(avg_2024, accuracy = 0.1)),
          ifelse(is.na(avg_2025), "N/A", comma(avg_2025, accuracy = 0.1)),
          ifelse(is.na(pct_change), "N/A", number(pct_change, accuracy = 0.1))
        )
      ) %>%
      addLegend(
        position = "bottomright",
        pal = pal,
        values = c(-20, 0, 20),
        title = "Percent Change (2025 vs 2024)\n(values capped at \u00b120%)",
        labFormat = labelFormat(suffix = "%")
      ) %>%
      addControl(
        html = paste0(
          "<div style='background: rgba(255,255,255,0.85); padding: 6px; font-size: 11px;'>",
          "<strong>Zone definitions</strong><br/>",
          "CP Zone: Manhattan south of approx. 60th Street (lat &lt; ", CP_LAT_THRESHOLD, ")<br/>",
          "Manhattan Outside Zone: remaining Manhattan<br/>",
          "Outer Boroughs: Bronx, Brooklyn, Queens, Staten Island",
          "</div>"
        ),
        position = "bottomleft"
      )
  })

  # ---- Time series aggregation reactive (daily/weekly) ----
  time_series_data <- reactive({
    df <- traffic_filtered()
    if (nrow(df) == 0) {
      return(data.frame())
    }
    agg_level <- if (is.null(input$agg_level)) "day" else input$agg_level
    build_time_series(df, agg_level = agg_level)
  })

  # ---- Before/After time series plot ----
  output$traffic_time_series <- renderPlot({
    ts <- time_series_data()

    if (nrow(ts) == 0) {
      return(
        ggplot() +
          annotate("text", x = 0, y = 0, label = "No data available for time series.", size = 5) +
          theme_void()
      )
    }

    ggplot(ts, aes(x = ts_date, y = avg_volume, color = zone_category)) +
      # Shaded annotation window after congestion pricing begins
      annotate(
        "rect",
        xmin = CP_START_DATE,
        xmax = CP_START_DATE + 30,
        ymin = -Inf,
        ymax = Inf,
        alpha = 0.05,
        fill = "red"
      ) +
      # Vertical intervention line on Jan 1, 2025
      geom_vline(
        xintercept = CP_START_DATE,
        linetype = "dashed",
        color = "red",
        size = 0.5
      ) +
      # Time series for each year
      geom_line(size = 0.8) +
      scale_color_manual(
        name = "Zone",
        values = c(
          "CP Zone" = "#d62728",
          "Manhattan Outside Zone" = "#1f77b4",
          "Outer Boroughs" = "#2ca02c"
        )
      ) +
      labs(
        x = "Date",
        y = "Average Peak-Hour Traffic Volume",
        title = "Average Peak-Hour Traffic Over Time by Zone",
        subtitle = "Weekdays only, 5 AM–9 PM (CP Zone vs Manhattan Outside vs Outer Boroughs)",
        color = "Zone"
      ) +
      # Text annotation describing congestion pricing start
      annotate(
        "text",
        x = CP_START_DATE + 15,
        y = Inf,
        label = "Congestion Pricing Begins",
        color = "red",
        size = 3,
        vjust = 2,
        hjust = 0
      ) +
      theme_minimal(base_size = 12) +
      theme(
        plot.title = element_text(face = "bold"),
        legend.position = "bottom"
      )
  })
}

# ---- Run Shiny app ----
shinyApp(ui = ui, server = server)

