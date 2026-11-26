import dagster as dg
from delay_prediction.datasets import latest_version, load_dataset
from delay_prediction.models import train_and_promote
from orchestration_dagster.defs.resources import S3_Resource


@dg.asset(
    # Trained on the latest dataset version, written by ml_training_dataset
    deps=["ml_training_dataset"],
    group_name="delay_prediction",
)
def ml_delay_model(s3_resource: S3_Resource) -> dg.MaterializeResult:
    """Train the delay prediction models, save the best and promote it to champion if it deserves it."""
    s3_client, bucket = s3_resource.get_client(), s3_resource.bucket_name
    version = latest_version(s3_client, bucket)
    splits, _ = load_dataset(version, s3_client, bucket)
    _, best, decision = train_and_promote(splits, version)
    return dg.MaterializeResult(
        metadata={
            "dataset_version": version,
            "best_model": best["model"],
            "f1": best["f1"],
            "test_f1": best["test_f1"],
            "mlflow_run_id": best["run_id"],
            "promotion": decision,
        }
    )
