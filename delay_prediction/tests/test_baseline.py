"""Tests of the baselines and metrics, on synthetic rows."""

import numpy as np
import polars as pl
import pytest
from delay_prediction.baseline import evaluate_baseline, predict
from delay_prediction.metrics import choose_threshold, evaluate


def frame(train_rates, route_rates, late) -> pl.DataFrame:
    """Rows with the two history features used by the baseline and the target."""
    return pl.DataFrame(
        {
            "train_late_rate_28d": train_rates,
            "route_late_rate_28d": route_rates,
            "is_late": late,
        },
        schema={
            "train_late_rate_28d": pl.Float64,
            "route_late_rate_28d": pl.Float64,
            "is_late": pl.Boolean,
        },
    )


def test_history_falls_back_on_route_then_training_rate():
    """No train history: line rate; no line history either: training rate."""
    rows = frame([0.8, None, None], [0.1, 0.4, None], [True, False, False])

    assert predict("history", rows, 0.25).tolist() == [0.8, 0.4, 0.25]


def test_constant_predicts_the_training_rate():
    """Every train gets the same probability."""
    rows = frame([0.8, None], [0.1, None], [True, False])

    assert predict("constant", rows, 0.25).tolist() == [0.25, 0.25]


def test_unknown_baseline_is_refused():
    """A typo in a baseline name fails instead of predicting nothing."""
    with pytest.raises(ValueError):
        predict("histroy", frame([0.1], [0.1], [True]), 0.25)


def test_perfect_ranking_and_random_ranking():
    """Late trains ranked first score 1; a constant scores 0.5 in ROC AUC."""
    y_true = np.array([1, 1, 0, 0, 0, 0])
    ranked = evaluate(y_true, np.array([0.9, 0.8, 0.2, 0.1, 0.1, 0.1]), 0.5)
    flat = evaluate(y_true, np.full(6, 1 / 3), 1 / 3)

    assert ranked["pr_auc"] == pytest.approx(1.0)
    assert ranked["precision"] == ranked["recall"] == 1.0
    assert flat["roc_auc"] == pytest.approx(0.5)
    assert flat["recall"] == 1.0 and flat["flagged_rate"] == 1.0


def test_threshold_separates_classes_when_possible():
    """The F1 threshold flags exactly the late trains when they are separable."""
    y_true = np.array([1, 1, 0, 0])
    proba = np.array([0.9, 0.7, 0.3, 0.1])

    threshold = choose_threshold(y_true, proba)

    assert 0.3 < threshold <= 0.7


def test_history_beats_constant_when_history_is_informative():
    """Trains late in the past are late again: history ranks them first."""
    train = frame([0.9, 0.9, 0.1, 0.1] * 5, [0.5] * 20, [True, True, False, False] * 5)
    valid = frame([0.9, 0.1, 0.1, 0.9], [0.5] * 4, [True, False, False, True])

    history = evaluate_baseline("history", train, valid)
    constant = evaluate_baseline("constant", train, valid)

    assert history["valid"]["pr_auc"] > constant["valid"]["pr_auc"]
    assert constant["threshold"] == pytest.approx(0.5)
