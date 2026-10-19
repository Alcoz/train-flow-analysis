# data-eng

Standalone library and warehouse for the SNCF data:

- `src/data_eng/`: Python functions that download the SNCF feeds and convert them into tables;
- `dbt_data_pipeline/`: dbt project that builds the Gold layer from Silver;
- `bigquery/`: SQL scripts that expose Gold in BigQuery.

data-eng schedules nothing and depends on no orchestrator: the caller (a script,
a scheduler) chains the steps below and writes their outputs to the bucket.

## Setup

```bash
uv sync                                   # from the repository root (uv workspace)
uv run pytest data-eng/tests              # unit tests, S3 mocked with moto
```

dbt reads the S3 endpoint and credentials from the environment variables
`S3_API`, `S3_ACCESS_KEY`, `S3_SECRET_ACCESS_KEY` and `REGION_NAME`
(see [`profiles.yml`](dbt_data_pipeline/profiles.yml)).

## Run order

Each step reads what the previous one wrote in the bucket.

| # | Step | With | Reads | Writes |
| --- | --- | --- | --- | --- |
| 1 | Download the feeds (Bronze) | [`sncf_getter.py`](src/data_eng/sncf_getter.py) | SNCF URLs | raw bytes, to store under `data/bronze/` |
| 2 | Convert to tables (Silver) | [`sncf_transformer.py`](src/data_eng/sncf_transformer.py) | Bronze | Polars dataframes, to store as Parquet under `data/silver/` |
| 3 | Build Gold, delays | dbt, `tag:daily` | Silver GTFS and trip updates | `dim_dates`, `dim_routes`, `fact_train_trips`, `agg_daily_route` |
| 4 | Build Gold, alerts | dbt, `tag:alerts` | Silver alerts, Gold `fact_train_trips` | `dim_alerts`, `fact_train_alerts` |
| 5 | Expose Gold in BigQuery | [`create_gold_external_tables.sql`](bigquery/create_gold_external_tables.sql) | Gold Parquet files | `gold.*` external tables |
| 6 | Weekly views | [`create_weekly_views.sql`](bigquery/create_weekly_views.sql) | `gold.*` tables | `gold.weekly_*` views |

### 1–2. Python: Bronze and Silver

```python
from data_eng.sncf_getter import get_sncf_trip_update_train_data
from data_eng.sncf_transformer import sncf_trip_updates_protobuf_to_sheets

snapshot = get_sncf_trip_update_train_data()                 # bytes (Bronze)
trip_updates = sncf_trip_updates_protobuf_to_sheets(snapshot)  # dataframe (Silver)
```

| Feed | Download | Convert |
| --- | --- | --- |
| Theoretical GTFS (once a day) | `get_sncf_theoretical_train_data()` | `extract_gtfs_files()`, then `gtfs_csv_to_dataframe()` per table |
| Trip updates (every few minutes) | `get_sncf_trip_update_train_data()` | `sncf_trip_updates_protobuf_to_sheets()` |
| Service alerts (every few minutes) | `get_sncf_service_alerts_data()` | `sncf_service_alerts_protobuf_to_sheets()` |

[`utils/s3_connector.py`](src/data_eng/utils/s3_connector.py) reads and writes the bucket.
dbt only reads Silver at the paths and with the partitioning described in
[docs/silver_tables.md](docs/silver_tables.md): the caller must write the files there.

### 3–4. dbt: Gold

dbt builds one day, given by the `today` var. Run `tag:daily` first:
`fact_train_alerts` (`tag:alerts`) reads the `fact_train_trips` file of the same day.

```bash
cd data-eng/dbt_data_pipeline
uv run dbt deps
uv run dbt build --select tag:daily --vars '{today: 2026-10-15}'
uv run dbt build --select tag:alerts --vars '{today: 2026-10-15}'
```

Build day D once the trip updates fetched on D+1 are in Silver: overnight trains
of day D keep sending updates after midnight.
Each run uses an in-memory DuckDB: models of the two tags only share their Gold files.

### 5–6. BigQuery

Run the two scripts in the BigQuery console, in this order, after at least one
day of Gold is built (BigQuery reads the schema of an external table from its files).

They are run **once**, and again only when a table or a view changes. The external
tables read the Parquet files at query time: a new Gold day is visible right away.
The scripts only create: after a model is renamed, drop the old table or view
(`DROP EXTERNAL TABLE` / `DROP VIEW`) and delete its old Gold folder.

## Documentation

- [docs/business_rules.md](docs/business_rules.md): indicators, train status, alert classification
- [docs/silver_tables.md](docs/silver_tables.md): Silver tables, paths and columns
- [dbt_data_pipeline/models/schema.yml](dbt_data_pipeline/models/schema.yml): Gold models, columns, tests and purpose
- [dbt_data_pipeline/models/exposures.yml](dbt_data_pipeline/models/exposures.yml): BigQuery views and the models they read
