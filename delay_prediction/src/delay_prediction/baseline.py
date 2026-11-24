"""Baselines of the delay prediction: the rules a trained model must beat.

- ``history``: the late rate of the train over the 28 days before D; the
  late rate of its line when the train has no history; the late rate of the
  training set when the line has none either.
- ``constant``: the late rate of the training set for every train. A model
  that does not beat it has learnt nothing from the features.

Nothing is fitted but the fallback rate and the threshold, both on the
training set. Each baseline is logged as its own MLflow run, tagged
``purpose=baseline``, so that it sits next to the trained models:

    uv run --env-file <env file> python -m delay_prediction.baseline --dataset latest
"""

import argparse
import os

import numpy as np
import polars as pl

from delay_prediction.datasets import DATASETS_PREFIX, TARGET, load_dataset
from delay_prediction.metrics import choose_threshold, evaluate

BASELINES = ("history", "constant")


def predict(name: str, frame: pl.DataFrame, train_late_rate: float) -> np.ndarray:
    """Return the probability of being late of each row for a baseline."""
    if name == "history":
        proba = frame.select(
            pl.coalesce(
                "train_late_rate_28d",
                "route_late_rate_28d",
                pl.lit(train_late_rate),
            )
        ).to_series()
        return proba.to_numpy().astype(float)
    if name == "constant":
        return np.full(frame.height, train_late_rate)
    raise ValueError(f"Unknown baseline: {name}")


def evaluate_baseline(name: str, train: pl.DataFrame, valid: pl.DataFrame) -> dict:
    """Fit the fallback rate and threshold on train, return train and valid metrics."""
    train_late_rate = float(train[TARGET].mean())
    y_train = train[TARGET].to_numpy()
    y_valid = valid[TARGET].to_numpy()
    train_proba = predict(name, train, train_late_rate)
    # A constant has a single threshold: every train is flagged or none
    threshold = (
        train_late_rate
        if name == "constant"
        else choose_threshold(y_train, train_proba)
    )
    return {
        "threshold": threshold,
        "train": evaluate(y_train, train_proba, threshold),
        "valid": evaluate(y_valid, predict(name, valid, train_late_rate), threshold),
    }


def log_baseline_to_mlflow(
    name: str,
    result: dict,
    train: pl.DataFrame,
    valid: pl.DataFrame,
    manifest: dict,
) -> str:
    """Log a baseline as a MLflow run and return its id."""
    import mlflow

    version = manifest["version"]
    source = (
        f"s3://{os.environ.get('BUCKET_NAME', '<bucket>')}/{DATASETS_PREFIX}/{version}"
    )
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])
    with mlflow.start_run(run_name=f"baseline_{name}") as run:
        mlflow.set_tags(
            {
                "purpose": "baseline",
                "model_type": name,
                "dataset_version": version,
            }
        )
        mlflow.log_params(
            {
                "baseline": name,
                "dataset_version": version,
                "dataset_digest": manifest["digest"],
                "threshold": round(result["threshold"], 4),
            }
        )
        for split in ("train", "valid"):
            mlflow.log_metrics(
                {f"{split}_{key}": value for key, value in result[split].items()}
            )
        # Same frames as the dataset run: MLflow gives them the same digest
        for split, frame in (("train", train), ("valid", valid)):
            dataset = mlflow.data.from_polars(
                frame,
                source=f"{source}/{split}.parquet",
                targets=TARGET,
                name=f"{version}_{split}",
            )
            mlflow.log_input(dataset, context=split)
    return run.info.run_id


def main() -> None:
    """Evaluate the baselines on a dataset version and log them to MLflow."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", default="latest", help="Dataset version")
    parser.add_argument(
        "--cache-dir",
        default="data/ml/datasets",
        help="Local copy of the dataset versions",
    )
    parser.add_argument(
        "--no-mlflow", action="store_true", help="Only print the metrics"
    )
    args = parser.parse_args()

    train, valid, manifest = load_dataset(args.dataset, cache_dir=args.cache_dir)
    print(f"Dataset {manifest['version']}: {train.height} train, {valid.height} valid")
    rows = []
    for name in BASELINES:
        result = evaluate_baseline(name, train, valid)
        run_id = None
        if not args.no_mlflow:
            run_id = log_baseline_to_mlflow(name, result, train, valid, manifest)
        rows.append(
            {"baseline": name, "threshold": round(result["threshold"], 3)}
            | {key: round(value, 3) for key, value in result["valid"].items()}
            | {"mlflow_run_id": run_id}
        )
    with pl.Config(tbl_cols=-1, tbl_width_chars=200):
        print("Validation metrics:")
        print(pl.DataFrame(rows))


if __name__ == "__main__":
    main()
