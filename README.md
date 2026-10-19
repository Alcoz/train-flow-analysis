# train-flow-analysis

A data platform that measures the punctuality of SNCF (French national railway) trains.

Every day, the SNCF publishes the **theoretical timetable** of its trains (GTFS). Every few minutes, it also publishes **real-time forecasts** of their delays (GTFS-RT trip updates). This project collects both feeds continuously, stores them in an S3 data lake, and builds a daily dimensional warehouse that tells, for each trip and each stop, how late the train was.

## How it works

The pipeline follows a **Bronze / Silver / Gold** (medallion) architecture, orchestrated with **Dagster**:

| Layer      | What it contains                                                                                     | Format             | Built by                     |
|------------|------------------------------------------------------------------------------------------------------|--------------------|------------------------------|
| **Bronze** | Raw files exactly as returned by the SNCF APIs: the daily GTFS zip archive and every GTFS-RT snapshot | `.zip`, `.pb`      | Dagster assets (Python)      |
| **Silver** | Cleaned and explicitly typed tables: one Parquet file per GTFS table, and flattened trip updates      | Parquet            | Dagster assets (Polars)      |
| **Gold**   | Star schema: date, route, stop and trip dimensions, plus a fact table of delays per trip and stop    | Parquet, partitioned by date | dbt + DuckDB      |

```
 SNCF GTFS (daily zip)          SNCF GTFS-RT (trip updates)
          │ 10:30 every day               │ every 2 minutes
          ▼                               ▼
 ┌─────────────────────────────────────────────────────────┐
 │ Bronze   data/bronze/theory/date=YYYY-MM-DD/sncf_gtfs.zip│
 │          data/bronze/continue/year=/month=/day=/HH-MM-SS.pb
 └─────────────────────────────────────────────────────────┘
          │ daily partition, built at 03:00 for the previous day
          ▼
 ┌─────────────────────────────────────────────────────────┐
 │ Silver   data/silver/theory/date=YYYY-MM-DD/<table>.parquet
 │          data/silver/continue/date=YYYY-MM-DD/*.parquet  │
 │          data/silver/alerts/date=YYYY-MM-DD/*.parquet    │
 └─────────────────────────────────────────────────────────┘
          │ dbt build (DuckDB, reads/writes S3 through httpfs)
          ▼
 ┌─────────────────────────────────────────────────────────┐
 │ Gold     data/gold/{dim_routes, fact_trip_runs,         │
 │            agg_daily_route, fact_alerts,                │
 │            fact_trip_disruptions}/date=…, dim_dates     │
 └─────────────────────────────────────────────────────────┘
```

### Ingestion (Bronze)

- `sncf_bronze_theoretical_data` downloads the [SNCF GTFS archive](https://ressources.data.sncf.com/explore/dataset/horaires-sncf/information/) once a day and stores it unchanged. An asset check warns (without blocking) when `trips.txt`, `routes.txt` or `stops.txt` is missing or empty.
- `sncf_bronze_continue_data` fetches the [GTFS-RT trip updates feed](https://transport.data.gouv.fr/) every 2 minutes and stores each protobuf snapshot.
- `sncf_bronze_service_alerts_data` fetches the GTFS-RT service alerts feed (disruptions with their cause, effect and affected routes, trips and stops) every 2 minutes and stores each snapshot under `data/bronze/alerts/year=/month=/day=/HH-MM-SS.pb`.

Raw data is kept as-is, so any later layer can be rebuilt from Bronze.

### Cleaning (Silver)

Silver assets are **partitioned by day** (`Europe/Paris`) and idempotent: rebuilding a partition overwrites the same files.

- `sncf_silver_theoretical_data` extracts the archive and writes each GTFS table (`trips`, `routes`, `stops`, `stop_times`, `transfers`, `calendar_dates`, `feed_info`, `agency`) as Parquet. Identifiers are kept as strings and only known numeric/date columns are cast, strictly.
- `sncf_silver_alerts_data` converts each service alerts snapshot of the day into one Parquet file under `data/silver/alerts/date=YYYY-MM-DD/`, with one row per alert: cause and effect (null when the SNCF did not set them), French header and description cleaned of their HTML, and the active periods and targeted train numbers as lists. Alerts have no service date, so they are stored by **fetch date**.
- `sncf_silver_continue_data` flattens the trip updates (one row per trip and stop, with departure/arrival times and delays). Data is stored by **service date** rather than fetch date: overnight trains keep sending updates after midnight, so the next day's snapshots are read too. This is why the daily job runs at 03:00 for the previous day.

### Warehouse (Gold)

The dbt project in [`data-eng/dbt_data_pipeline`](data-eng/dbt_data_pipeline) runs on DuckDB and writes external Parquet tables back to S3, partitioned by date:

- `dim_dates` — calendar dimension (via `dbt_date`)
- `dim_routes` — daily snapshot of routes, with terminal stations parsed from the route name
- `fact_trip_runs` — one row per train of the service date (announced or added in real time), with its status and delay at the effective terminus
- `agg_daily_route` — daily counts per line, behind the weekly delay and cancellation views
- `fact_alerts` — service alerts of the day, classified by category and reason
- `fact_trip_disruptions` — one row per alert and affected train, behind the weekly disruption views

Models enforce contracts and are covered by dbt tests (uniqueness, not-null, accepted values, relationships). Each model states its `purpose` and `status` in `meta`, and [`exposures.yml`](data-eng/dbt_data_pipeline/models/exposures.yml) declares the BigQuery views that read them: a model with no exposure downstream is unused. Dagster loads the dbt project as assets, so the whole lineage from Bronze to Gold is visible in one graph.

### Jobs and schedules

| Job                          | Assets                       | Schedule (Europe/Paris)            |
|------------------------------|------------------------------|------------------------------------|
| `theoretical_getter_job`     | Bronze GTFS                  | Every day at 10:30                 |
| `trip_update_getter_job`     | Bronze GTFS-RT               | Every 2 minutes                    |
| `service_alerts_getter_job`  | Bronze GTFS-RT alerts        | Every 2 minutes                    |
| `sncf_delay_preparation_job` | Silver GTFS and GTFS-RT, Gold delay and cancellation models (partitions since 2026-09-14) | Every day at 03:00, previous day's partition |
| `sncf_alerts_preparation_job` | Silver alerts, `fact_alerts` (partitions since 2026-10-07, start of the alerts collection) | Every day at 03:00, previous day's partition |
| `sncf_disruptions_preparation_job` | `fact_trip_disruptions`, from the Gold files of the two jobs above | Every day at 03:30, previous day's partition |

A job has a single partitions definition, so assets starting on different dates are in different jobs. The DuckDB of each dbt run is in memory: jobs only share Gold files. The dbt project is loaded by two Dagster components (`dbt_ingest` for `tag:daily`, `dbt_alerts` for `tag:alerts`), one per partitions definition.

## Project structure

```
train-flow-analysis/
├── data-eng/                       # Python library + dbt project
│   ├── src/data_eng/
│   │   ├── sncf_getter.py          # Download GTFS and GTFS-RT feeds (trip updates, alerts)
│   │   ├── sncf_transformer.py     # GTFS → typed Polars tables, protobuf → rows
│   │   └── utils/s3_connector.py   # boto3 helpers (get, put, paginated listing)
│   ├── dbt_data_pipeline/          # dbt project: Gold star schema
│   └── tests/                      # Unit tests (S3 mocked with moto)
├── orchestration-dagster/          # Dagster project
│   ├── src/orchestration_dagster/defs/
│   │   ├── assets/                 # Bronze and Silver assets
│   │   ├── dbt_ingest/             # dbt project loaded as Dagster assets
│   │   ├── jobs/  schedules/       # Jobs and their schedules
│   │   ├── partitions.py           # Daily partitions
│   │   └── resources.py            # S3 resource
│   ├── dagster.yaml                # Postgres run/event storage
│   └── workspace.yaml              # gRPC code location for Docker
├── Dockerfile_sncf_orchestrator    # Dagster code server (user code)
├── Dockerfile_dagster              # Dagster webserver and daemon
├── docker-compose.yaml             # Orchestrator, webserver and daemon services
├── deploy.sh                       # Remote deployment over SSH
├── Makefile                        # Developer commands
└── pyproject.toml                  # uv workspace (data-eng, orchestration-dagster)
```

## Tech stack

- **Python 3.12**, managed with [uv](https://docs.astral.sh/uv/) as a workspace
- **[Dagster](https://dagster.io/)** — assets, partitions, asset checks, jobs, schedules, dbt integration
- **[Polars](https://pola.rs/)** — Silver transformations
- **[dbt](https://www.getdbt.com/)** + **[DuckDB](https://duckdb.org/)** — Gold warehouse, read from and written to S3
- **S3-compatible object storage** (MinIO locally, any S3 endpoint in production) via `boto3`
- **Postgres** — Dagster run and event storage
- **Docker Compose** — production deployment
- **ruff**, **pre-commit**, **pytest** + **moto** — code quality and tests
- **GitHub Actions** — lint, tests and SSH deployment

## Getting started

### Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/getting-started/installation/)
- An S3-compatible bucket (e.g. a local [MinIO](https://min.io/))
- Docker and Docker Compose, to run the full stack

### Configuration

Create a `.env` file at the root of the repository:

```bash
# S3 storage
S3_API=http://localhost:9000
S3_ACCESS_KEY=...
S3_SECRET_ACCESS_KEY=...
REGION_NAME=...
BUCKET_NAME=sncf-bucket

# Dagster storage (Postgres)
DAGSTER_POSTGRES_USER=...
DAGSTER_POSTGRES_PASSWORD=...
DAGSTER_POSTGRES_DB=...
DAGSTER_POSTGRES_HOST=...
```

> The dbt sources and the Gold location currently point to `s3://sncf-bucket`; use this bucket name or update `models/sources.yml` and `profiles.yml`.

### Installation

```bash
uv sync
```

### Run locally

```bash
make minio-run          # start a local MinIO server
make dagster-postgres   # start the Postgres backend for Dagster
make dagster-run        # start Dagster in development mode
```

Then open [http://localhost:3000](http://localhost:3000) to materialize assets, launch backfills or enable the schedules.

dbt can also be run on its own:

```bash
make dbt-build          # run + test (or make dbt-run / make dbt-test)
```

### Run with Docker

```bash
make docker-build
make docker-up
```

This starts three services: the code server (`train_orchestrator`), the Dagster webserver (port 3000) and the Dagster daemon, which runs the schedules.

## Development

```bash
uv run pytest           # unit tests
uvx ruff check .        # lint
uvx ruff format .       # format
```

`pre-commit` hooks run ruff before each commit. On GitHub Actions, every push and pull request runs the linter and the tests; pushes to `main` are then deployed to the server with [`deploy.sh`](deploy.sh), which pulls the repository and restarts the Docker Compose stack.

## License

No license specified.
