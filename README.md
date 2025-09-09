# GTFS Simulator (Go)

## Overview

- Reads a GTFS dataset from PostgreSQL/PostGIS (imported with [postgis-gtfs-importer](https://github.com/mobidata-bw/postgis-gtfs-importer)) and simulates active vehicles based on calendar/calendar_dates and stop_times.
- Publishes simulated positions to NATS with subject `routeId.tripId`.
- Each vehicle (trip) runs in its own goroutine.

## Configuration

- Create a `.env` file (see `.env.example`). Supported variables:
  - `DATABASE_URL` or `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGSSLMODE` (+ `PGDATABASE`)
  - `CITY` (or `CITY_NAME`) – if set, the simulator connects to the cluster's `postgres` DB, queries `public.latest_successful_imports` for the newest `db_name` matching the city (ILIKE), and uses that DB. Ensure your base DSN points to the same cluster and `PGDATABASE=postgres` (or use a `DATABASE_URL` with `/postgres`).
  - `NATS_URL` (default `nats://127.0.0.1:4222`)
  - `LOG_NATS_SUBJECTS` (default off) – when true, logs each NATS subject published
  - `METRICS_ADDR` (e.g., `:9102`) – when set, exposes Prometheus metrics at `/metrics`
  - `PUBLISH_INTERVAL_MS` (default `1000`)
  - `TRIPS_REFRESH_INTERVAL_SEC` (default `60`) – how often to re-query active trips and start new ones
  - `TRIPS_PRELOAD_MINUTES` (default `30`) – schedule trips that start within this horizon to begin automatically at their exact start time
  - `SPEED_MULTIPLIER` (default `1.0`)
  - `TZ` (optional time zone, e.g., `Europe/Vilnius`)

## Build & Run

```bash
go mod tidy
go build ./cmd/simulator
./simulator
```

## Notes

- Active trips are selected for the current service date using `calendar` and `calendar_dates`.
- Simulation uses the `shapes` polyline of each trip; progress is computed from the first to last stop time and interpolated along the shape.
- Segment speeds follow stop_times: movement between stops uses per-stop arrival/departure times. Distance along the shape between stops uses `stop_times.shape_dist_traveled` when available; otherwise, stops are mapped to the nearest position on the trip shape using stop coordinates. If that is unavailable too, distances are distributed evenly. Dwell times (arrival != departure) produce pauses at stops.
- If `shape_dist_traveled` is present in `shapes` or `stop_times`, it is used; otherwise, haversine distances are computed between shape points.
- Vehicle ID is set to the trip ID to remain deterministic and consistent; subjects are sanitized to avoid illegal NATS tokens.
- When `CITY` is set, the simulator re-checks `public.latest_successful_imports` every 30 minutes and on DB ping failures. If a newer `db_name` is found or the current DB is unavailable, it gracefully switches to the new database and restarts simulations.
- Active trips are re-queried every `TRIPS_REFRESH_INTERVAL_SEC`; newly active trips start automatically and finished trips stop on their end time.
- Trips starting within `TRIPS_PRELOAD_MINUTES` are pre-scheduled to start exactly at their start time, reducing DB polling load.

## Observability

- Prometheus metrics (enable with `METRICS_ADDR`):
  - Gauges: `simulator_active_trips`, `simulator_scheduled_trips`, `simulator_nats_connected`, `simulator_speed_multiplier`, `simulator_publish_interval_seconds`, `simulator_refresh_interval_seconds`, `simulator_preload_horizon_minutes`.
  - Counters: `simulator_trips_started_total`, `simulator_trips_finished_total`, `simulator_trips_scheduled_total`, `simulator_nats_published_total`, `simulator_nats_publish_errors_total`, `simulator_db_switches_total{reason}`.
  - Histograms: `simulator_tick_duration_seconds`, `simulator_publish_duration_seconds`.

## Dockerized Prometheus + Grafana

- Files are under `observability/` and provision a ready‑made Grafana dashboard.
- Steps:
  1) Ensure the simulator runs with `METRICS_ADDR=:9102` on your host.
  2) On macOS/Windows, Prometheus is preconfigured to scrape `host.docker.internal:9102`.
     - On Linux, edit `observability/prometheus/prometheus.yml` and replace with your host IP (e.g., `172.17.0.1:9102`).
  3) Start the stack:
     - `docker compose -f observability/docker-compose.yml up -d`
  4) Open Prometheus at <http://localhost:9090> and Grafana at <http://localhost:3000> (admin/admin).
  5) The Grafana dashboard “GTFS Simulator” is auto‑provisioned.

## Project Layout

- `cmd/simulator/main.go` – wiring: config, DB, NATS, start/stop.
- `internal/config` – load `.env` and env vars.
- `internal/db` – GTFS queries and geometry helpers.
- `internal/sim` – manager and trip goroutines.
- `internal/publisher` – NATS publisher and JSON payload.
