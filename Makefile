minio-run:
	minio server /Users/baptistedarnala/Documents/Database/minio/ --license ~/Documents/Database/minio/minio.license

dagster-run:
	uv run dg dev

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

create-sncf-bucket: 
	gcloud storage buckets create gs://sncf-bucket --default-storage-class=STANDARD --location=EUROPE-WEST9 --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention

docker-build:
	direnv exec / docker compose --env-file .env.docker up --build 

dagster-postgres:
	docker run --name dagster-postgres -p 5432:5432 -e POSTGRES_USER=${DAGSTER_POSTGRES_USER} -e POSTGRES_PASSWORD=${DAGSTER_POSTGRES_PASSWORD} -e POSTGRES_DB=${DAGSTER_POSTGRES_DB} -d postgres