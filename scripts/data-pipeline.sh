#!/bin/bash

uv run scripts/data-pipeline-bronze-to-silver.py

export $(cat .env | xargs)

make dbt-run

export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="$REGION_NAME"

aws s3 cp warehouse/sncf_data_analysis.duckdb s3://sncf-bucket/data/gold/warehouse/sncf_data_analysis.duckdb --endpoint-url http://127.0.0.1:9000