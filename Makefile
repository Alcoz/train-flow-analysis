minio-run:
	minio server /Users/baptistedarnala/Documents/Database/minio/ --license ~/Documents/Database/minio/minio.license

dagster-run:
	dg dev

dbt-run:
	dbt run --project-dir data-eng/dbt_data_pipeline --profiles-dir data-eng/dbt_data_pipeline

dbt-test:
	dbt test --project-dir data-eng/dbt_data_pipeline --profiles-dir data-eng/dbt_data_pipeline

dbt-build:
	dbt build --project-dir data-eng/dbt_data_pipeline --profiles-dir data-eng/dbt_data_pipeline

duckui:
	duckdb -ui warehouse/sncf_data_analysis.duckdb

data-pipeline:
	sh scripts/data-pipeline.sh