"""Tests of the champion / challenger promotion, on a local MLflow registry."""

import mlflow
import pytest
from delay_prediction.datasets import TARGET
from delay_prediction.models import (
    build_models,
    evaluate_model,
    features,
    promote_if_better,
    save_best_model,
    with_threshold,
)
from delay_prediction.registry import champion_metrics, promote, promotion_refusal
from test_models import evaluation_in_new_run, trains_with_nulls

MODEL_NAME = "delay_prediction"


def challenger(f1: float, test_f1: float) -> dict:
    """Return the evaluation of a best model, with its validation and test F1."""
    return {"f1": f1, "test_f1": test_f1}


def test_challenger_beating_baseline_and_champion_is_promoted():
    """No refusal when the baseline and the champion are beaten and test holds."""
    assert promotion_refusal(challenger(0.48, 0.47), 0.47, 0.46) is None


def test_first_challenger_only_needs_to_beat_the_baseline():
    """Without champion, beating the best baseline is enough."""
    assert promotion_refusal(challenger(0.48, 0.47), 0.47, None) is None


@pytest.mark.parametrize(
    ("evaluation", "champion", "reason"),
    [
        (challenger(0.47, 0.47), None, "baseline"),
        (challenger(0.48, 0.48), 0.49, "champion"),
        (challenger(0.48, 0.42), 0.46, "test F1"),
    ],
)
def test_challenger_is_refused(evaluation, champion, reason):
    """Refused when it does not beat the baseline (0.47), the champion, or test drops by more than 0.05."""
    assert reason in promotion_refusal(evaluation, 0.47, champion)


@pytest.mark.usefixtures("local_mlflow")
def test_promoted_model_becomes_the_champion():
    """No champion before the first promotion; then the champion is measured on valid."""
    train, valid = trains_with_nulls(200, seed=0), trains_with_nulls(100, seed=1)
    model = build_models()["logistic_regression"]
    threshold, metrics = evaluate_model(model, train, valid)
    with mlflow.start_run():
        model_uri = mlflow.sklearn.log_model(
            with_threshold(model, threshold),
            name="model",
            skops_trusted_types=["numpy.dtype"],
        ).model_uri

    assert champion_metrics(MODEL_NAME, features(valid), valid[TARGET]) is None
    assert promote(model_uri, MODEL_NAME) == "1"
    assert champion_metrics(
        MODEL_NAME, features(valid), valid[TARGET]
    ) == pytest.approx(metrics)


@pytest.mark.usefixtures("local_mlflow")
def test_champion_is_logged_as_a_run_on_the_new_validation_set(monkeypatch):
    """The precision, recall and F1 of the champion on valid are a run tagged purpose=champion."""
    monkeypatch.setenv("MLFLOW_REGISTERED_MODEL_NAME", MODEL_NAME)
    # The test trains have no date, needed by the baselines
    monkeypatch.setattr(
        "delay_prediction.models.evaluate_baselines",
        lambda train, valid: {"baseline": ({"f1": 0.0}, {})},
    )
    splits = {
        "train": trains_with_nulls(200, seed=0),
        "valid": trains_with_nulls(100, seed=1),
        "test": trains_with_nulls(100, seed=2),
    }
    first = evaluation_in_new_run(
        "logistic_regression", splits["train"], splits["valid"]
    )
    promote(
        save_best_model([first], splits["test"], "2026-11-25_145528")["model_uri"],
        MODEL_NAME,
    )
    challenger = evaluation_in_new_run(
        "logistic_regression", splits["train"], splits["valid"]
    )
    challenger = save_best_model([challenger], splits["test"], "2026-11-26_124042")

    promote_if_better(challenger, splits, "2026-11-26_124042")

    [run] = mlflow.search_runs(
        filter_string="tags.purpose = 'champion'", output_format="list"
    )
    assert run.data.tags["dataset_version"] == "2026-11-26_124042"
    assert run.data.metrics == pytest.approx(
        champion_metrics(MODEL_NAME, features(splits["valid"]), splits["valid"][TARGET])
    )
