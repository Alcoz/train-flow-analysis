"""Tests of the training datasets, on a mocked S3 bucket with synthetic data."""

import io
from datetime import date, timedelta

import polars as pl
import pytest
from data_eng.utils.s3_connector import connect_to_s3
from delay_prediction.datasets import (
    FEATURE_COLUMNS,
    TARGET,
    add_target,
    build_dataset,
    load_dataset,
    write_dataset,
)
from moto import mock_aws

BUCKET = "test-bucket"
FIRST_DATE = date(2026, 10, 1)
DAYS = 10


def features_row(day: date, trip_id: str, train_number: str) -> dict:
    """One row of ml_delay_features."""
    return {
        "date": day,
        "trip_id": trip_id,
        "train_number": train_number,
        "route_id": "R1",
        "gtfs_version": day - timedelta(days=1),
    } | {column: 1 for column in FEATURE_COLUMNS}


def put_parquet(s3_client, key: str, table: pl.DataFrame) -> None:
    """Write a table as Parquet to the mocked bucket."""
    buffer = io.BytesIO()
    table.write_parquet(buffer)
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


@pytest.fixture
def s3_client():
    """Provide a bucket with DAYS days of two trains: A late on even days, B never late.

    The features also have the next day, which has no trains yet.
    """
    with mock_aws():
        s3_client = connect_to_s3(None, "testing", "testing", "eu-west-1")
        s3_client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        for day_index in range(DAYS + 1):
            day = FIRST_DATE + timedelta(days=day_index)
            features = pl.DataFrame(
                [features_row(day, "A", "A"), features_row(day, "B", "B")]
            ).drop("date")
            put_parquet(
                s3_client,
                f"data/gold/ml_delay_features/date={day}/data_0.parquet",
                features,
            )
            if day_index < DAYS:
                trains = pl.DataFrame(
                    {
                        "trip_id": ["A", "B"],
                        "train_number": ["A", "B"],
                        "is_observed": [True, True],
                        "is_cancelled": [False, False],
                        "terminus_arrival_delay": [600 if day_index % 2 == 0 else 0, 0],
                    }
                )
                put_parquet(
                    s3_client,
                    f"data/gold/fact_train_trips/date={day}/data_0.parquet",
                    trains,
                )
        yield s3_client


def test_build_dataset_splits_by_date(s3_client):
    """Each day with trains is in one set only: train, then valid, then test."""
    splits = build_dataset(s3_client, BUCKET, FIRST_DATE, valid_days=3, test_days=2)
    train, valid, test = splits["train"], splits["valid"], splits["test"]

    assert train.height + valid.height + test.height == DAYS * 2
    assert valid["date"].n_unique() == 3
    assert test["date"].n_unique() == 2
    assert train["date"].max() < valid["date"].min()
    assert valid["date"].max() < test["date"].min()
    late_days = train.filter(pl.col("trip_id") == "A")[TARGET].to_list()
    assert late_days == [day_index % 2 == 0 for day_index in range(DAYS - 5)]


def test_written_dataset_is_loaded_as_latest(s3_client):
    """A written version is the latest one and loads unchanged."""
    splits = build_dataset(s3_client, BUCKET, FIRST_DATE, valid_days=3, test_days=2)
    version = write_dataset(s3_client, BUCKET, splits)

    loaded_splits, loaded_version = load_dataset(s3_client=s3_client, bucket=BUCKET)

    assert loaded_version == version
    for split_name, split in splits.items():
        assert loaded_splits[split_name].equals(split)


def test_unobserved_and_cancelled_trains_are_left_out():
    """Only trains seen running, not cancelled, with a delay get a target."""
    day = FIRST_DATE
    features = pl.DataFrame([features_row(day, trip_id, trip_id) for trip_id in "ABCD"])
    trains = pl.DataFrame(
        {
            "date": [day] * 4,
            "trip_id": ["A", "B", "C", "D"],
            "train_number": ["A", "B", "C", "D"],
            "is_observed": [True, False, True, True],
            "is_cancelled": [False, False, True, False],
            "terminus_arrival_delay": [600, None, None, None],
        }
    )

    assert add_target(features, trains)["trip_id"].to_list() == ["A"]


def test_changed_trip_id_is_matched_by_single_train_number():
    """A train whose trip_id changed is matched by its number, if unique that day."""
    day = FIRST_DATE
    features = pl.DataFrame(
        [features_row(day, "OLD_A", "NUM_A"), features_row(day, "OLD_B", "NUM_B")]
    )
    trains = pl.DataFrame(
        {
            "date": [day] * 3,
            # NUM_B is held by two trains of the day
            "trip_id": ["NEW_A", "NEW_B1", "NEW_B2"],
            "train_number": ["NUM_A", "NUM_B", "NUM_B"],
            "is_observed": [True] * 3,
            "is_cancelled": [False] * 3,
            "terminus_arrival_delay": [600, 0, 0],
        }
    )

    labelled = add_target(features, trains)

    assert labelled["trip_id"].to_list() == ["OLD_A"]
    assert labelled[TARGET].to_list() == [True]
