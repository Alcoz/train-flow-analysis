
# train-flow-analysis

A data platform for analyzing SNCF (French national railway) train punctuality and delays, built on a modern Bronze / Silver / Gold data lakehouse architecture. The project ingests both theoretical (GTFS) and real-time (GTFS-RT) train schedule data from the SNCF open data API, transforms it into an analytics-ready warehouse, and exposes it through an interactive dashboard.

## Overview

The pipeline follows a medallion architecture:

1. **Bronze** — Raw theoretical (GTFS) and real-time trip update (GTFS-RT) data is fetched from the SNCF API and stored as-is in S3-compatible object storage (MinIO / GCS).
2. **Silver** — Raw data is cleaned and converted into structured, typed files.
3. **Gold** — dbt builds a dimensional warehouse (dimensions and facts) on top of the Silver layer using DuckDB, ready for analytics.

The whole pipeline is orchestrated with **Dagster**, and the resulting warehouse is explored through a **Streamlit** dashboard.

## Data sources

https://ressources.data.sncf.com/explore/dataset/horaires-sncf/information/

https://docs.aws.amazon.com/boto3/latest/reference/services/s3.html

## Architecture

```
SNCF API (GTFS + GTFS-RT)
        │
        ▼
  Dagster assets  ──►  Bronze (raw) ──►  Silver (cleaned) ──►  Gold (dbt / DuckDB warehouse)
        │                                                            │
        ▼                                                            ▼
  S3 / MinIO storage                                     Streamlit data-viz platform
```

## Project structure

```
train-flow-analysis/
├── data-eng/                  # Data extraction & transformation library (SNCF getters, S3 connector, bronze→silver logic)
├── orchestration-dagster/     # Dagster project: assets, jobs, schedules, partitions and resources
├── data-viz-platform/         # Streamlit application to explore train delays
├── scripts/                   # Pipeline entry-point scripts (bronze→silver, dbt run, S3 upload)
├── Dockerfile_dagster
├── Dockerfile_sncf_orchestrator
├── docker-compose.yaml        # Dagster webserver, daemon and orchestrator services
├── Makefile                   # Common developer commands
└── pyproject.toml             # uv workspace definition
```

The `data-eng/dbt_data_pipeline` folder contains the dbt project that builds the warehouse:

- `models/warehouse/`: `dim_routes`, `dim_trips`, `fact_train_trips`
- `models/analytics/`: `train_delay` — average delay per route

## Tech stack

- **Python 3.12**, managed with [`uv`](https://docs.astral.sh/uv/) (workspace with `data-eng`, `data-viz-platform`, and `orchestration-dagster` as members)
- **[Dagster](https://dagster.io/)** for pipeline orchestration (assets, jobs, schedules, partitions)
- **[dbt](https://www.getdbt.com/)** for SQL-based transformations
- **[DuckDB](https://duckdb.org/)** as the analytical warehouse engine
- **S3-compatible storage** (MinIO locally, GCS/AWS in production) via `boto3`
- **[Streamlit](https://streamlit.io/)** for the data visualization front end
- **Docker / Docker Compose** for containerized orchestration services
- **ruff**, **pre-commit** and **pytest** for linting, formatting and testing

## Data sources

- [SNCF horaires-sncf dataset](https://ressources.data.sncf.com/explore/dataset/horaires-sncf/information/) — theoretical and real-time train schedule data
- [boto3 / S3 documentation](https://docs.aws.amazon.com/boto3/latest/reference/services/s3.html) — used for object storage integration

## Getting started

### Prerequisites

- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
- Docker & Docker Compose (for running Dagster and MinIO)
- An `.env` file with the required environment variables (S3 credentials, Dagster Postgres settings, etc.)

### Installation

Install all workspace dependencies with `uv`:

```bash
uv sync
```

### Running the pipeline

Start local object storage (MinIO):

```bash
make minio-run
```

Launch the Dagster UI in development mode:

```bash
make dagster-run
```

Open [http://localhost:3000](http://localhost:3000) to view and materialize assets.

Alternatively, run the full stack (orchestrator + Dagster webserver + daemon) with Docker Compose:

```bash
make docker-build
```

### Running the dbt transformations

```bash
make dbt-run     # run models
make dbt-test    # run tests
make dbt-build   # run + test
```

### Running the end-to-end pipeline script

```bash
make data-pipeline
```

This runs the bronze-to-silver transformation, executes the dbt models, and uploads the resulting DuckDB warehouse to the Gold layer on S3.

### Exploring the warehouse

Open the DuckDB warehouse in the DuckDB UI:

```bash
make duckui
```

Or launch the Streamlit dashboard (see `scripts/data-viz-platform.py`) to visualize average train delays per route.

## Development

Common developer commands are defined in the `Makefile`, including creating the SNCF storage bucket and starting the Dagster Postgres backend. Code quality is enforced with `ruff` (linting and formatting) and `pre-commit` hooks, and unit tests are run with `pytest`. Linting and unit tests are also checked in CI via GitHub Actions (`.github/workflows/lint.yml` and `unittest.yml`).

## License

No license specified.