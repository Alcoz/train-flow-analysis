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
    results_table,
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


@pytest.mark.usefixtures("local_mlflow")
@pytest.mark.parametrize("name", build_models())
def test_best_model_is_saved_with_its_threshold(name):
    """Only the best model is saved and measured on test; reloaded, it predicts late above its threshold."""
    train, valid = trains_with_nulls(200, seed=0), trains_with_nulls(100, seed=1)
    test = trains_with_nulls(100, seed=2)
    best = evaluation_in_new_run(name, train, valid)
    worse = evaluation_in_new_run("logistic_regression", train, valid) | {"f1": -1.0}

    saved = save_best_model([worse, best], test, "2026-11-25_145528")
    model_uri = f"runs:/{best['run_id']}/model"
    saved_model = mlflow.sklearn.load_model(model_uri)

    assert saved["run_id"] == best["run_id"]
    assert mlflow.models.get_model_info(model_uri).metadata == {
        "training_dataset": "2026-11-25_145528",
        "training_dataset_date": "2026-11-25",
        "training_dataset_time": "14:55:28",
    }
    assert not mlflow.artifacts.list_artifacts(run_id=worse["run_id"])
    assert not mlflow.get_run(worse["run_id"]).data.metrics
    logged_test_f1 = mlflow.get_run(best["run_id"]).data.metrics["test_f1"]
    assert logged_test_f1 == pytest.approx(saved["test_f1"])
    assert saved["test_f1"] > 0.9
    late_probabilities = best["trained_model"].predict_proba(features(valid))[:, 1]
    expected = late_probabilities >= best["threshold"]
    assert (saved_model.predict(features(valid)) == expected).all()


def test_results_table_puts_each_test_metric_next_to_its_validation_metric():
    """Columns go metric, test_metric; only the best model has test metrics."""
    evaluations = [
        {
            "model": "a",
            "threshold": 0.2,
            "precision": 0.3,
            "recall": 0.6,
            "f1": 0.4,
            "run_id": "1",
        },
        {
            "model": "b",
            "threshold": 0.3,
            "precision": 0.4,
            "recall": 0.7,
            "f1": 0.5,
            "run_id": "2",
        },
    ]
    best = evaluations[1] | {
        "test_precision": 0.35,
        "test_recall": 0.65,
        "test_f1": 0.45,
    }

    table = results_table(evaluations, best)

    assert table.columns == [
        "model",
        "threshold",
        "precision",
        "test_precision",
        "recall",
        "test_recall",
        "f1",
        "test_f1",
    ]
    assert table["test_f1"].to_list() == [None, 0.45]
