"""Tests of the models, on synthetic trains with nulls."""

import mlflow
import numpy as np
import polars as pl
import pytest
from delay_prediction.datasets import NUMERIC_FEATURE_COLUMNS, TARGET
from delay_prediction.models import (
    build_models,
    evaluate_model,
    features,
    save_best_model,
)


def trains_with_nulls(rows: int, seed: int) -> pl.DataFrame:
    """Trains late when their 28 day late rate is above 0.5; one rate in five is null."""
    generator = np.random.default_rng(seed)
    late_rate = generator.random(rows)
    trains = pl.DataFrame(
        {column: generator.random(rows) for column in NUMERIC_FEATURE_COLUMNS}
    ).with_columns(
        train_type=pl.Series(generator.choice(["Train TER", "TGV INOUI"], rows)),
        train_late_rate_28d=pl.Series(late_rate),
        **{TARGET: pl.Series(late_rate > 0.5)},
    )
    return trains.with_columns(
        train_late_rate_7d=pl.when(pl.int_range(rows) % 5 == 0)
        .then(None)
        .otherwise(pl.col("train_late_rate_7d"))
    )


@pytest.mark.parametrize("name", build_models())
def test_model_learns_with_nulls(name):
    """Every model trains with nulls and finds the late trains."""
    model = build_models()[name]

    threshold, metrics = evaluate_model(
        model, trains_with_nulls(200, seed=0), trains_with_nulls(100, seed=1)
    )

    assert 0 <= threshold <= 1
    assert metrics["f1"] > 0.9


def evaluation_in_new_run(name: str, train, valid) -> dict:
    """Train and evaluate a model, with an empty MLflow run to save it in."""
    model = build_models()[name]
    threshold, metrics = evaluate_model(model, train, valid)
    with mlflow.start_run() as run:
        pass
    return (
        {"model": name, "threshold": threshold}
        | metrics
        | {"run_id": run.info.run_id, "trained_model": model}
    )


@pytest.mark.parametrize("name", build_models())
def test_best_model_is_saved_with_its_threshold(name, tmp_path, monkeypatch):
    """Only the model with the best F1 is saved; reloaded, it predicts late above its threshold."""
    monkeypatch.delenv("MLFLOW_REGISTRY_URI", raising=False)
    mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
    mlflow.set_experiment(
        experiment_id=mlflow.create_experiment(
            "test", (tmp_path / "artifacts").as_uri()
        )
    )
    train, valid = trains_with_nulls(200, seed=0), trains_with_nulls(100, seed=1)
    best = evaluation_in_new_run(name, train, valid)
    worse = evaluation_in_new_run("logistic_regression", train, valid) | {"f1": -1.0}

    saved = save_best_model([worse, best])
    saved_model = mlflow.sklearn.load_model(f"runs:/{best['run_id']}/model")

    assert saved is best
    assert not mlflow.artifacts.list_artifacts(run_id=worse["run_id"])
    late_probabilities = best["trained_model"].predict_proba(features(valid))[:, 1]
    expected = late_probabilities >= best["threshold"]
    assert (saved_model.predict(features(valid)) == expected).all()
