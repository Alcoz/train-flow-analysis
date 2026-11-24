from datetime import date

import dagster as dg
from delay_prediction.datasets import (
    DEFAULT_FIRST_DATE,
    DEFAULT_TEST_DAYS,
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
    test_days: int = DEFAULT_TEST_DAYS


@dg.asset(
    # Not partitioned: each materialization writes a new version
    deps=["ml_delay_features", "fact_train_trips"],
    group_name="delay_prediction",
)
def ml_training_dataset(
    config: TrainingDatasetConfig, s3_resource: S3_Resource
) -> dg.MaterializeResult:
    """Write a new training dataset version of the delay prediction and log it to MLflow."""
    s3_client, bucket = s3_resource.get_client(), s3_resource.bucket_name
    splits = build_dataset(
        s3_client,
        bucket,
        date.fromisoformat(config.first_date),
        config.valid_days,
        config.test_days,
    )
    version = write_dataset(s3_client, bucket, splits)
    mlflow_run_id = log_dataset_to_mlflow(splits, version, bucket)
    return dg.MaterializeResult(
        metadata={"version": version, "mlflow_run_id": mlflow_run_id}
        | {f"{split_name}_rows": split.height for split_name, split in splits.items()}
    )
