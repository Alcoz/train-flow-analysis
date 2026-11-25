"""Baselines of the delay prediction: the scores a trained model must beat.

- train_rate_7d_above_28d: a train is predicted late when it was late more often over the
  last 7 days than over the last 28 days. A train without history is
  predicted on time.
- late_rate_above_0.5: a train is predicted late when its late rate over
  28 days (else its line's over 28 days, else the network's on D-1) is
  above 0.5.
- late_rate_above_best_f1_threshold: same late rate, compared to the threshold with the
  best F1 score on the training set.
- all_trains_late: every train is predicted late (DummyClassifier), the score
  obtained without any information on the trains.

Each baseline is logged as a MLflow run tagged purpose=baseline:

    make train-baseline
"""

import os

import mlflow
import polars as pl
from sklearn.dummy import DummyClassifier

from delay_prediction.datasets import TARGET, load_dataset, log_dataset_inputs
from delay_prediction.metrics import best_f1_threshold, classification_metrics


def evaluate_history_train(valid: pl.DataFrame):
    """Return the metrics on valid of the history rule."""
    predicted_late = (
        valid["train_late_rate_7d"] > valid["train_late_rate_28d"]
    ).fill_null(False)
    return classification_metrics(valid[TARGET], predicted_late)


def evaluate_history_route(valid: pl.DataFrame):
    """Return the metrics on valid of the history rule."""
    predicted_late = (
        valid["route_late_rate_7d"] > valid["route_late_rate_28d"]
    ).fill_null(False)
    return classification_metrics(valid[TARGET], predicted_late)


def late_rate(trains: pl.DataFrame) -> pl.Series:
    """Return the late rate of each train, else of its line, else of the network."""
    return trains.select(
        pl.coalesce(
            "train_late_rate_28d", "route_late_rate_28d", "network_late_rate_1d"
        )
    ).to_series()


def evaluate_late_rate_above_half(valid: pl.DataFrame):
    """Return the metrics on valid when a train is late if its late rate is above 0.5."""
    predicted_late = (late_rate(valid) > 0.5).fill_null(False)
    return classification_metrics(valid[TARGET], predicted_late)


def evaluate_late_rate_best_f1(train: pl.DataFrame, valid: pl.DataFrame):
    """Return the threshold with the best F1 on train and the metrics on valid."""
    threshold = best_f1_threshold(train[TARGET], late_rate(train).fill_null(0))
    predicted_late = (late_rate(valid) >= threshold).fill_null(False)
    return threshold, classification_metrics(valid[TARGET], predicted_late)


def evaluate_all_late(train: pl.DataFrame, valid: pl.DataFrame):
    """Return the metrics on valid when every train is predicted late."""
    model = DummyClassifier(strategy="constant", constant=True)
    model.fit(train.select("date"), train[TARGET])
    return classification_metrics(valid[TARGET], model.predict(valid.select("date")))


def log_baseline(name: str, metrics: dict, splits: dict, version: str, params=None):
    """Log a baseline as a MLflow run."""
    with mlflow.start_run(run_name=f"baseline_{name}"):
        mlflow.set_tags({"purpose": "baseline", "dataset_version": version})
        mlflow.log_params(params or {})
        mlflow.log_metrics(metrics)
        log_dataset_inputs(splits, version, os.environ["BUCKET_NAME"])


def main() -> None:
    """Evaluate the baselines on the latest dataset and log them to MLflow."""
    splits, version = load_dataset()
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])

    history_train_metrics = evaluate_history_train(splits["valid"])
    log_baseline("train_rate_7d_above_28d", history_train_metrics, splits, version)
    history_route_metrics = evaluate_history_route(splits["valid"])
    log_baseline("route_rate_7d_above_28d", history_route_metrics, splits, version)
    above_half_metrics = evaluate_late_rate_above_half(splits["valid"])
    log_baseline("late_rate_above_0.5", above_half_metrics, splits, version)
    threshold, best_f1_metrics = evaluate_late_rate_best_f1(
        splits["train"], splits["valid"]
    )
    log_baseline(
        "late_rate_above_best_f1_threshold",
        best_f1_metrics,
        splits,
        version,
        {"threshold": threshold},
    )
    all_late_metrics = evaluate_all_late(splits["train"], splits["valid"])
    log_baseline("all_trains_late", all_late_metrics, splits, version)

    print(f"Dataset {version}, validation metrics:")
    print(
        pl.DataFrame(
            [
                {"baseline": "train_rate_7d_above_28d"} | history_train_metrics,
                {"baseline": "route_rate_7d_above_28d"} | history_route_metrics,
                {"baseline": "late_rate_above_0.5"} | above_half_metrics,
                {"baseline": "late_rate_above_best_f1_threshold"} | best_f1_metrics,
                {"baseline": "all_trains_late"} | all_late_metrics,
            ]
        )
    )


if __name__ == "__main__":
    main()
