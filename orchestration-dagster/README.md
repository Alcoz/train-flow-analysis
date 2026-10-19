# orchestration-dagster

Dagster project that runs the SNCF pipeline on a schedule. It depends on
[`data-eng`](../data-eng/README.md): it calls its Python functions to write Bronze
and Silver to the bucket, and runs its dbt project to build Gold. data-eng does
not depend on this project.

## Structure

```
src/orchestration_dagster/
├── definitions.py              # Loads everything under defs/
└── defs/
    ├── assets/
    │   ├── sncf_getter_assets.py         # Bronze: download the feeds to S3
    │   └── sncf_transfo_silver_assets.py # Silver: convert Bronze to Parquet
    ├── dbt_ingest/             # dbt models tagged `daily` (delays)
    ├── dbt_alerts/             # dbt models tagged `alerts` (alerts)
    ├── partitions.py           # Daily partitions (Europe/Paris)
    ├── jobs/  schedules/       # Jobs and their schedules
    ├── sensors/                # Discord and Healthchecks alerting
    └── resources.py            # S3 resource
dagster.yaml                    # Instance: Postgres storage, retention, backfill concurrency
workspace.yaml                  # gRPC code location, used by the Docker stack
```

## Assets

| Layer | Asset | Writes in the bucket | Partitioned |
| --- | --- | --- | --- |
| Bronze | `sncf_bronze_theoretical_data` | `data/bronze/theory/date=D/sncf_gtfs.zip` | no |
| Bronze | `sncf_bronze_continue_data` | `data/bronze/continue/year=/month=/day=/` | no |
| Bronze | `sncf_bronze_service_alerts_data` | `data/bronze/alerts/year=/month=/day=/` | no |
| Silver | `sncf_silver_theoretical_data` (one asset per GTFS table) | `data/silver/theory/date=D/` | daily |
| Silver | `sncf_silver_continue_data` | `data/silver/continue/date=D/` | daily |
| Silver | `sncf_silver_alerts_data` | `data/silver/alerts/date=D/` | alerts |
| Gold | dbt models (`dbt_ingest`, `dbt_alerts`) | `data/gold/<model>/date=D/` | daily / alerts |

Silver paths and columns: [data-eng/docs/silver_tables.md](../data-eng/docs/silver_tables.md).

Two partitions definitions (`partitions.py`):

- `daily_partitions`, from 2026-09-14: delays;
- `alerts_partitions`, from 2026-10-07, when alerts started to be collected.

A dbt component is a single multi-asset with a single partitions definition, so the
dbt project is loaded twice: `dbt_ingest` builds `tag:daily`, `dbt_alerts` builds
`tag:alerts`. Both pass the partition key to dbt as the `today` var.

## Jobs and schedules (Europe/Paris)

Order of the jobs, from their dependencies:

```
1. Getters (continuous)          theoretical / trip_update / service_alerts → Bronze
        │
        ▼  day D+1
2. sncf_delay_preparation_job  ┐  in parallel, 03:00
   sncf_alerts_preparation_job ┘
        │  (both finished)
        ▼
3. sncf_train_alerts_preparation_job   03:30
        │
        ▼  once, after the first Gold days
4. BigQuery: create_gold_external_tables.sql, then create_weekly_views.sql
```

| Job | Builds | Schedule |
| --- | --- | --- |
| `theoretical_getter_job` | Bronze GTFS | every day at 10:30 |
| `trip_update_getter_job` | Bronze trip updates | every 2 minutes |
| `service_alerts_getter_job` | Bronze alerts | every 5 minutes |
| `sncf_delay_preparation_job` | Silver GTFS and trip updates, Gold `tag:daily` | 03:00, previous day's partition |
| `sncf_alerts_preparation_job` | Silver alerts, Gold `dim_alerts` | 03:00, previous day's partition |
| `sncf_train_alerts_preparation_job` | Gold `fact_train_alerts` | 03:30, previous day's partition |
| `purge_old_runs_job` | deletes getter runs older than 14 days | 04:00 |

- Day D is built on D+1: the Silver trip updates of D also read the snapshots fetched after midnight (overnight trains).
- `sncf_train_alerts_preparation_job` reads the Gold files of the day written by the two 03:00 jobs: each job runs dbt in its own in-memory DuckDB, so jobs only share Gold files.
- To rebuild a period, backfill the two 03:00 jobs first, then `sncf_train_alerts_preparation_job` on the same partitions. Backfill runs go one at a time (`dagster.yaml`), so the getters keep running.

## Sensors

Running by default, they write nothing:

- `discord_on_run_failure` / `discord_on_run_recovery`: Discord message on the first failure of a job, and when it works again;
- `healthchecks_heartbeat`: pings Healthchecks.io after each successful scheduled run; Healthchecks alerts when pings stop (daemon stuck, server down).

## Configuration

Environment variables, in the `.env` of the repository root:

| Variable | Used by |
| --- | --- |
| `S3_API`, `S3_ACCESS_KEY`, `S3_SECRET_ACCESS_KEY`, `REGION_NAME`, `BUCKET_NAME` | S3 resource and dbt |
| `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_DB`, `DAGSTER_POSTGRES_HOST` | Dagster storage (`dagster.yaml`) |
| `DISCORD_WEBHOOK_URL`, `HEALTHCHECK_PING_KEY` | Sensors, optional |

## Run

From the repository root:

```bash
uv sync
make dagster-run        # dg dev, UI on http://localhost:3000
uv run pytest orchestration-dagster/tests
```

In production, the Docker stack (`make docker-up`) runs the code server, the webserver
and the daemon, which launches the schedules and sensors. After the first Gold days are
built, the BigQuery scripts of data-eng must be run once: see
[data-eng/README.md](../data-eng/README.md#67-bigquery).
