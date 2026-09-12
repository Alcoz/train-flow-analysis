#!/bin/sh
set -e

cd /opt/dagster/app/data-eng/dbt_data_pipeline
uv run dbt deps
uv run dbt parse

cd /opt/dagster/app/orchestration-dagster/src/orchestration_dagster
exec "$@"