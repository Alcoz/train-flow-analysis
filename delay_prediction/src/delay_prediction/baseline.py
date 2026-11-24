"""Baselines of the delay prediction: the scores a trained model must beat.

- history: a train is predicted late when it was late more often over the
  last 7 days than over the last 28 days. A train without history is
  predicted on time.
- all_late: every train is predicted late (DummyClassifier), the score
  obtained without any information on the trains.

Each baseline is logged as a MLflow run tagged purpose=baseline:

    make train-baseline
"""

import os

import mlflow
import polars as pl
from sklearn.dummy import DummyClassifier

from delay_prediction.datasets import TARGET, load_dataset, log_dataset_inputs
from delay_prediction.metrics import classification_metrics


def evaluate_history(valid: pl.DataFrame):
    """Return the metrics on valid of the history rule."""
    predicted_late = (
        valid["train_late_rate_7d"] > valid["train_late_rate_28d"]
    ).fill_null(False)
    return classification_metrics(valid[TARGET], predicted_late)


def evaluate_all_late(train: pl.DataFrame, valid: pl.DataFrame):
    """Return the metrics on valid when every train is predicted late."""
    model = DummyClassifier(strategy="constant", constant=True)
    model.fit(train.select("date"), train[TARGET])
    return classification_metrics(valid[TARGET], model.predict(valid.select("date")))


def log_baseline(name: str, metrics: dict, splits: dict, version: str):
    """Log a baseline as a MLflow run."""
    with mlflow.start_run(run_name=f"baseline_{name}"):
        mlflow.set_tags({"purpose": "baseline", "dataset_version": version})
        mlflow.log_metrics(metrics)
        log_dataset_inputs(splits, version, os.environ["BUCKET_NAME"])


def main() -> None:
    """Evaluate the baselines on the latest dataset and log them to MLflow."""
    splits, version = load_dataset()
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])

    history_metrics = evaluate_history(splits["valid"])
    log_baseline("history", history_metrics, splits, version)
    all_late_metrics = evaluate_all_late(splits["train"], splits["valid"])
    log_baseline("all_late", all_late_metrics, splits, version)

    print(f"Dataset {version}, validation metrics:")
    print(
        pl.DataFrame(
            [
                {"baseline": "history"} | history_metrics,
                {"baseline": "all_late"} | all_late_metrics,
            ]
        )
    )


if __name__ == "__main__":
    main()
