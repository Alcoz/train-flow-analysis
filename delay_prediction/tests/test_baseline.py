"""Tests of the baselines and metrics, on synthetic trains."""

import polars as pl
import pytest
from delay_prediction.baseline import (
    evaluate_all_late,
    evaluate_history_train,
    evaluate_late_rate_above_half,
    evaluate_late_rate_best_f1,
    late_rate,
)
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

    metrics = evaluate_history_train(valid)

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


def rates(train_rates, route_rates, network_rates, is_late) -> pl.DataFrame:
    """Trains with their late rates of the train, line and network and their target."""
    return pl.DataFrame(
        {
            "train_late_rate_28d": train_rates,
            "route_late_rate_28d": route_rates,
            "network_late_rate_1d": network_rates,
            "is_late": is_late,
        },
        schema_overrides={
            "train_late_rate_28d": pl.Float64,
            "route_late_rate_28d": pl.Float64,
            "network_late_rate_1d": pl.Float64,
        },
    )


def test_late_rate_takes_train_then_line_then_network():
    """Without train rate: line rate; without line rate: network rate."""
    valid = rates([0.8, None, None], [0.1, 0.4, None], [0.2, 0.2, 0.3], [True] * 3)

    assert late_rate(valid).to_list() == [0.8, 0.4, 0.3]


def test_late_rate_above_half():
    """Late when the late rate is above 0.5."""
    valid = rates([0.8, 0.3], [None, None], [0.2, 0.2], [True, False])

    metrics = evaluate_late_rate_above_half(valid)

    assert metrics["precision"] == metrics["recall"] == 1.0


def test_late_rate_best_f1_threshold_is_chosen_on_train():
    """The threshold separating late trains on train is applied to valid."""
    train = rates(
        [0.9, 0.7, 0.3, 0.1], [None] * 4, [0.2] * 4, [True, True, False, False]
    )
    valid = rates([0.8, 0.2], [None, None], [0.2, 0.2], [True, False])

    threshold, metrics = evaluate_late_rate_best_f1(train, valid)

    assert 0.3 < threshold <= 0.7
    assert metrics["f1"] == pytest.approx(1.0)
