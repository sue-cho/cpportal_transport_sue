-- Run this in Supabase: SQL Editor → New query → paste → Run
-- Creates tables for London CCZ, Singapore city traffic, and real-time snapshots.

-- London: monthly vehicle counts entering the Congestion Charge Zone
CREATE TABLE IF NOT EXISTS public.london_ccz_monthly (
  id            bigserial PRIMARY KEY,
  month         date NOT NULL,
  metric_type   text NOT NULL CHECK (metric_type IN ('confirmed_vehicles', 'camera_captures')),
  vehicles      integer NOT NULL,
  charging_days integer,
  source_version text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (month, metric_type)
);

COMMENT ON TABLE public.london_ccz_monthly IS 'Monthly counts of vehicles entering London CCZ (confirmed_vehicles from Oct 2016, camera_captures from Jul 2010)';

-- Singapore: annual average daily traffic volume entering the city
CREATE TABLE IF NOT EXISTS public.singapore_city_annual (
  id                 bigserial PRIMARY KEY,
  year               integer NOT NULL UNIQUE,
  avg_daily_vehicles  numeric NOT NULL,
  time_window_desc    text,
  source_version     text,
  created_at         timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.singapore_city_annual IS 'Annual average daily traffic entering the city (7:30–19:00 weekdays), 2004–present';

-- TfL road status snapshots (for real-time conditions over time)
CREATE TABLE IF NOT EXISTS public.tfl_road_status_snapshots (
  id                bigserial PRIMARY KEY,
  snapshot_time     timestamptz NOT NULL,
  corridor_id       text NOT NULL,
  corridor_name     text,
  status_severity    text,
  status_description text,
  json_raw           jsonb,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tfl_snapshot_time ON public.tfl_road_status_snapshots (snapshot_time);
CREATE INDEX IF NOT EXISTS idx_tfl_corridor ON public.tfl_road_status_snapshots (corridor_id);

COMMENT ON TABLE public.tfl_road_status_snapshots IS 'Periodic snapshots of TfL road corridor status for trend analysis';

-- LTA Traffic Speed Bands snapshots
CREATE TABLE IF NOT EXISTS public.lta_speed_band_snapshots (
  id             bigserial PRIMARY KEY,
  snapshot_time  timestamptz NOT NULL,
  road_segment_id text,
  road_name      text,
  speed_band     integer,
  road_category  text,
  json_raw       jsonb,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lta_snapshot_time ON public.lta_speed_band_snapshots (snapshot_time);

COMMENT ON TABLE public.lta_speed_band_snapshots IS 'Periodic snapshots of LTA Traffic Speed Bands for trend analysis';
