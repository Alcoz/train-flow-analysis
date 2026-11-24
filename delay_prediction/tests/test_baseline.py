"""Tests of the baselines and metrics, on synthetic trains."""

import polars as pl
import pytest
from delay_prediction.baseline import evaluate_all_late, evaluate_history
from delay_prediction.metrics import classification_metrics


def trains(late_rates_7d, late_rates_28d, is_late) -> pl.DataFrame:
    """Trains with their late rates over 7 and 28 days and their target."""
    return pl.DataFrame(
        {
            "date": [None] * len(is_late),
            "train_late_rate_7d": late_rates_7d,
            "train_late_rate_28d": late_rates_28d,
            "is_late": is_late,
        },
        schema_overrides={
            "date": pl.Date,
            "train_late_rate_7d": pl.Float64,
            "train_late_rate_28d": pl.Float64,
        },
    )


def test_classification_metrics():
    """3 trains predicted late, 2 of them late, out of 4 late trains."""
    metrics = classification_metrics(
        [True, True, False, True, True, False],
        [True, True, True, False, False, False],
    )

    assert metrics["precision"] == pytest.approx(2 / 3)
    assert metrics["recall"] == pytest.approx(2 / 4)
    assert metrics["f1"] == pytest.approx(4 / 7)


def test_history_predicts_late_when_last_week_is_worse_than_last_month():
    """Late when the 7 day rate is above the 28 day rate; no history means on time."""
    valid = trains(
        [0.6, 0.2, None],
        [0.3, 0.3, None],
        [True, False, True],
    )

    metrics = evaluate_history(valid)

    # Predicted late: the first train only, which is late; the third is missed
    assert metrics["precision"] == pytest.approx(1.0)
    assert metrics["recall"] == pytest.approx(1 / 2)


def test_all_late_predicts_every_train_late():
    """Recall is 1 and precision is the share of late trains."""
    train = trains([0.1] * 4, [0.1] * 4, [True, False, False, False])
    valid = trains([0.1] * 4, [0.1] * 4, [True, True, False, False])

    metrics = evaluate_all_late(train, valid)

    assert metrics["recall"] == 1.0
    assert metrics["precision"] == pytest.approx(0.5)
