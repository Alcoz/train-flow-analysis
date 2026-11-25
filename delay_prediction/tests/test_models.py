"""Tests of the models, on synthetic trains with nulls."""

import numpy as np
import polars as pl
import pytest
from delay_prediction.datasets import NUMERIC_FEATURE_COLUMNS, TARGET
from delay_prediction.models import build_models, evaluate_model


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
