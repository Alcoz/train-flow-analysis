"""Check that MLflow reaches the Databricks tracking server and registry.

Logs one run tagged ``purpose=connection_check`` in the experiment, then
lists the registered models visible in Unity Catalog. Reads its settings
from the environment only (see the MLflow section of ``.env.example``):

    uv run --env-file .env python -m delay_prediction.check_tracking
"""

import os
import sys

import mlflow
from mlflow import MlflowClient

REQUIRED_VARIABLES = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_REGISTRY_URI",
    "MLFLOW_EXPERIMENT_NAME",
)


def missing_variables() -> list[str]:
    """Return the required environment variables that are unset or empty."""
    return [name for name in REQUIRED_VARIABLES if not os.environ.get(name)]


def main() -> int:
    """Log a connection-check run and list the registered models."""
    missing = missing_variables()
    if missing:
        print(f"Missing environment variables: {', '.join(missing)}")
        return 1

    # Tracking and registry URIs are read by MLflow from the environment
    experiment = mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])
    with mlflow.start_run(run_name="connection_check") as run:
        mlflow.set_tag("purpose", "connection_check")
    print(f"Run logged: {run.info.run_id} in experiment {experiment.experiment_id}")

    models = MlflowClient().search_registered_models(max_results=10)
    print(f"Registry reachable: {len(models)} registered model(s) visible")
    for model in models:
        print(f"  - {model.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
