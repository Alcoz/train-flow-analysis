import os
from datetime import date

import dagster as dg
from delay_prediction.datasets import (
    DEFAULT_FIRST_DATE,
    DEFAULT_VALID_DAYS,
    build_dataset,
    log_dataset_to_mlflow,
    write_dataset,
)
from orchestration_dagster.defs.resources import S3_Resource


class TrainingDatasetConfig(dg.Config):
    """Days used by a training dataset version."""

    first_date: str = DEFAULT_FIRST_DATE.isoformat()
    valid_days: int = DEFAULT_VALID_DAYS


@dg.asset(
    # Reads every partition of both tables: not partitioned itself, each
    # materialization writes a new frozen version
    deps=["ml_delay_features", "fact_train_trips"],
    group_name="delay_prediction",
)
def ml_training_dataset(
    context: dg.AssetExecutionContext,
    config: TrainingDatasetConfig,
    s3_resource: S3_Resource,
) -> dg.MaterializeResult:
    """Freeze a training dataset version of the delay prediction.

    Joins the features of each day to its target, splits by date and writes
    the version under data/ml/datasets/<version>/ (see delay_prediction.datasets).
    The version is also logged to MLflow when MLFLOW_TRACKING_URI is set.
    """
    client = s3_resource.get_client()
    bucket = s3_resource.bucket_name
    version = build_dataset(
        client,
        bucket,
        first_date=date.fromisoformat(config.first_date),
        valid_days=config.valid_days,
    )
    prefix = write_dataset(client, bucket, version)
    context.log.info(f"Dataset version {version.name} written to {prefix}")

    if os.environ.get("MLFLOW_TRACKING_URI"):
        mlflow_run_id = log_dataset_to_mlflow(version, f"s3://{bucket}/{prefix}")
    else:
        mlflow_run_id = "not logged: MLFLOW_TRACKING_URI is not set"
        context.log.warning(f"Dataset version {version.name} {mlflow_run_id}")

    manifest = version.manifest
    return dg.MaterializeResult(
        metadata={
            "version": version.name,
            "path": prefix,
            "mlflow_run_id": mlflow_run_id,
            "train_dates": f"{manifest['train']['first_date']} -> {manifest['train']['last_date']}",
            "valid_dates": f"{manifest['valid']['first_date']} -> {manifest['valid']['last_date']}",
            "train_rows": manifest["train"]["rows"],
            "valid_rows": manifest["valid"]["rows"],
            "train_late_rate": manifest["train"]["late_rate"],
            "valid_late_rate": manifest["valid"]["late_rate"],
            "dropped_rows": manifest["dropped_rows"],
        }
    )
