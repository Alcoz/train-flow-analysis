"""Tests of the daily prediction and of its monitoring, on a mocked bucket and a local MLflow."""

import io
from datetime import date

import mlflow
import polars as pl
import pytest
from data_eng.utils.s3_connector import connect_to_s3, send_object_to_s3
from delay_prediction.datasets import TARGET
from delay_prediction.models import build_models, evaluate_model, with_threshold
from delay_prediction.monitoring import (
    failed_tests,
    monitoring_reports,
    save_reports,
    with_target,
)
from delay_prediction.predictions import (
    CHAMPION_VERSION,
    PREDICTION,
    predict,
    predict_day,
    read_predictions,
)
from delay_prediction.registry import promote
from evidently.ui.workspace import Workspace
from moto import mock_aws
from test_models import trains_with_nulls

BUCKET = "test-bucket"
MODEL_NAME = "delay_prediction"
DAY = date(2026, 11, 25)


def day_features(rows: int, seed: int) -> pl.DataFrame:
    """Return the features of the trains of DAY, with their keys and ids."""
    return (
        trains_with_nulls(rows, seed)
        .drop(TARGET)
        .with_columns(
            date=pl.lit(DAY),
            trip_id=pl.format("trip-{}", pl.int_range(rows)),
            train_number=pl.format("{}", pl.int_range(rows)),
            route_id=pl.lit("route-1"),
            gtfs_version=pl.lit("2026-11-24"),
        )
    )


def trained_champion():
    """Return a logistic regression with its threshold, trained on synthetic trains."""
    model = build_models()["logistic_regression"]
    threshold, _ = evaluate_model(
        model, trains_with_nulls(200, seed=0), trains_with_nulls(100, seed=1)
    )
    return with_threshold(model, threshold)


@pytest.fixture
def bucket():
    """Provide an S3 client on a mocked bucket."""
    with mock_aws():
        s3_client = connect_to_s3(
            endpoint_url="https://s3.eu-west-1.amazonaws.com",
            access_key_id="testing",
            secret_access_key="testing",
            region_name="eu-west-1",
        )
        s3_client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        yield s3_client


@pytest.mark.usefixtures("local_mlflow")
def test_predictions_of_the_day_are_written_with_the_champion_version(bucket):
    """The champion predicts every train of the day; its version is in the file."""
    with mlflow.start_run():
        model_uri = mlflow.sklearn.log_model(
            trained_champion(), name="model", skops_trusted_types=["numpy.dtype"]
        ).model_uri
    version = promote(model_uri, MODEL_NAME)
    buffer = io.BytesIO()
    day_features(50, seed=2).write_parquet(buffer)
    key = f"data/gold/ml_delay_features/date={DAY}/data_0.parquet"
    send_object_to_s3(bucket, BUCKET, buffer.getvalue(), key)

    predict_day(bucket, BUCKET, MODEL_NAME, DAY)

    predictions = read_predictions(bucket, BUCKET, DAY)
    assert predictions.height == 50
    assert predictions[CHAMPION_VERSION].unique().to_list() == [version]
    assert predictions["date"].unique().to_list() == [DAY]


def test_target_is_added_to_the_predictions_of_the_trains_that_ran():
    """Only the trains seen running get their target, next to their prediction."""
    predictions = predict(trained_champion(), day_features(3, seed=2), "1")
    trains = pl.DataFrame(
        {
            "date": [DAY, DAY],
            "trip_id": ["trip-0", "trip-1"],
            "train_number": ["0", "1"],
            "is_observed": [True, False],
            "is_cancelled": [False, False],
            "terminus_arrival_delay": [600, None],
        }
    )

    current = with_target(predictions, trains)

    assert current["trip_id"].to_list() == ["trip-0"]
    assert current[TARGET].to_list() == [True]
    assert PREDICTION in current.columns


def test_failed_f1_test_is_reported_and_reports_are_saved(tmp_path, monkeypatch):
    """A F1 below the minimum fails its test; both reports go to one project."""
    monkeypatch.setenv("EVIDENTLY_WORKSPACE", str(tmp_path))
    reference = trains_with_nulls(300, seed=0)
    current = trains_with_nulls(200, seed=1).with_columns(
        pl.col(TARGET).not_().alias(PREDICTION)
    )

    snapshots = monitoring_reports(reference, current, min_f1=0.5, day=DAY)
    save_reports(snapshots)

    assert [test for test in failed_tests(snapshots) if "F1" in test]
    [project] = Workspace(str(tmp_path)).list_projects()
    assert len(Workspace(str(tmp_path)).list_runs(project.id)) == 2
