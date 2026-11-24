# delay_prediction

Predicts whether an SNCF train will reach its terminus **5 minutes late or more**, the day before it runs. Features come from the Gold table `ml_delay_features` (one row per train of day D, known before D starts), the target from `fact_train_trips` of day D. Rules: [`data-eng/docs/business_rules.md`](../data-eng/docs/business_rules.md).

## Training datasets

A dataset version is a frozen copy of the labelled features, built by the Dagster job `sncf_ml_training_dataset_job` (asset `ml_training_dataset`, launched by hand) with [`datasets.py`](src/delay_prediction/datasets.py):

| Rule | Value |
| --- | --- |
| Rows | Days from `first_date` (default 2026-10-13, the first day with 28 days of history) that have both their features and their trains. The current day has no target yet and is left out. |
| Target | `is_late`: `terminus_arrival_delay >= 300` seconds. Only trains seen running, not cancelled and with a measured delay are kept: the others have no observed answer (`dropped_rows` in the manifest). |
| Matching | On `(date, trip_id)`. The features take the trip_id of the GTFS version of D-1, `fact_train_trips` the one of the last version announcing the train: when the SNCF changed it in between (about 2% of the rows), the train is matched by its number, if no other train of D holds it. |
| Split | By date: the last `valid_days` days (default 14) are the validation set, the days before the training set. |
| Storage | `data/ml/datasets/<version>/` on the bucket: `train.parquet`, `valid.parquet`, `manifest.json` (written last: a version without a manifest is incomplete and ignored). |
| Version | `YYYY-MM-DD_HHMMSS_<digest>`: UTC build time, then the start of the SHA-256 digest of the data. `latest` is the last one. |
| MLflow | Each version is logged as a run tagged `purpose=training_dataset`, with its dates, row counts, late rates and both sets as inputs. Only metadata goes to MLflow: the data stays on the bucket. Skipped when `MLFLOW_TRACKING_URI` is not set. |

Load a version, in Dagster or locally:

```python
from delay_prediction.datasets import load_dataset

train, valid, manifest = load_dataset("latest", cache_dir="data/ml/datasets")
```

Without a client, `load_dataset` reads the bucket with the `S3_*` and `BUCKET_NAME` variables (locally: `uv run --env-file <env file> python ...`). With `cache_dir`, a version is copied there on its first read and read from there afterwards; `data/` is ignored by git.

## MLflow

Tracking and registry are on Databricks (variables in `.env.example`). Check the connection:

```bash
uv run --env-file .env python -m delay_prediction.check_tracking
```

In Docker, the orchestrator receives the same variables from docker-compose.
