-- Exposes the gold layer written by dbt in GCS as BigQuery external tables.
-- Nothing is copied: every query reads the Parquet files currently in the bucket,
-- so new or rebuilt partitions are visible as soon as Dagster writes them.
--
-- Run once in the BigQuery console (tables use the console's current project).
-- Safe to re-run: every statement is CREATE OR REPLACE / IF NOT EXISTS.
--
-- The dataset location must match the bucket's location
-- (gcloud storage buckets describe gs://sncf-bucket --format="value(location)").
-- @@location must be the script's first statement, otherwise the console runs it
-- in its default region (US) and rejects the EU dataset.

SET @@location = 'EU';

CREATE SCHEMA IF NOT EXISTS gold
OPTIONS (location = 'EU');


-- Partitioned tables: gs://sncf-bucket/data/gold/<table>/date=YYYY-MM-DD/data_0.parquet
-- DuckDB drops the partition column from the files, so `date` only comes
-- from the folder name through hive partitioning.

CREATE OR REPLACE EXTERNAL TABLE gold.fact_train_trips
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/fact_train_trips/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/fact_train_trips'
);

CREATE OR REPLACE EXTERNAL TABLE gold.fact_trip_runs
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/fact_trip_runs/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/fact_trip_runs'
);

CREATE OR REPLACE EXTERNAL TABLE gold.agg_daily_route
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/agg_daily_route/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/agg_daily_route'
);

-- active_periods and informed_entities are lists of structs: list inference
-- reads them as ARRAY<STRUCT> instead of the raw Parquet list wrapper.
CREATE OR REPLACE EXTERNAL TABLE gold.fact_alerts
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  enable_list_inference = true,
  uris = ['gs://sncf-bucket/data/gold/fact_alerts/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/fact_alerts'
);

CREATE OR REPLACE EXTERNAL TABLE gold.fact_trip_disruptions
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/fact_trip_disruptions/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/fact_trip_disruptions'
);

CREATE OR REPLACE EXTERNAL TABLE gold.dim_trips
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/dim_trips/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/dim_trips'
);

CREATE OR REPLACE EXTERNAL TABLE gold.dim_routes
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/dim_routes/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/dim_routes'
);

CREATE OR REPLACE EXTERNAL TABLE gold.dim_stops
WITH PARTITION COLUMNS (date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/dim_stops/*'],
  hive_partition_uri_prefix = 'gs://sncf-bucket/data/gold/dim_stops'
);


-- Not partitioned: dbt-duckdb writes a single file at {external_root}/dim_dates.parquet

CREATE OR REPLACE EXTERNAL TABLE gold.dim_dates
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sncf-bucket/data/gold/dim_dates.parquet']
);
