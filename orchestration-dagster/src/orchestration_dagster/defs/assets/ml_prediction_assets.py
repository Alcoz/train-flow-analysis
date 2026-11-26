import os
from datetime import date

import dagster as dg
from delay_prediction.monitoring import monitor_day
from delay_prediction.predictions import CHAMPION_VERSION, PREDICTION, predict_day
from orchestration_dagster.defs.partitions import (
    daily_partitions,
    ml_features_partitions,
)
from orchestration_dagster.defs.resources import S3_Resource
from orchestration_dagster.defs.sensors.alert_sensors import post_to_discord


@dg.asset(
    partitions_def=ml_features_partitions,
    deps=["ml_delay_features"],
    group_name="delay_prediction",
)
def ml_delay_predictions(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
) -> dg.MaterializeResult:
    """Predict which trains of the day arrive late, with the current champion."""
    predictions = predict_day(
        s3_resource.get_client(),
        s3_resource.bucket_name,
        os.environ["MLFLOW_REGISTERED_MODEL_NAME"],
        date.fromisoformat(context.partition_key),
    )
    return dg.MaterializeResult(
        metadata={
            CHAMPION_VERSION: predictions[CHAMPION_VERSION][0],
            "trains": predictions.height,
            "predicted_late_rate": predictions[PREDICTION].mean(),
        }
    )


@dg.asset(
    partitions_def=daily_partitions,
    # The trains of the day must have run: fact_train_trips of the day is built
    # the day after
    deps=["ml_delay_predictions", "fact_train_trips"],
    group_name="delay_prediction",
)
def ml_delay_monitoring(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
) -> dg.MaterializeResult:
    """Compare the predictions of the day to the delays with Evidently; alert Discord on failed tests."""
    summary = monitor_day(
        s3_resource.get_client(),
        s3_resource.bucket_name,
        os.environ["MLFLOW_REGISTERED_MODEL_NAME"],
        date.fromisoformat(context.partition_key),
    )
    if summary["failed_tests"]:
        failures = "\n".join(f"- {test}" for test in summary["failed_tests"])
        post_to_discord(
            f"🟠 **ml_delay_monitoring {context.partition_key}** "
            f"(champion version {summary[CHAMPION_VERSION]})\n{failures}"
        )
    return dg.MaterializeResult(
        metadata=summary | {"failed_tests": len(summary["failed_tests"])}
    )
