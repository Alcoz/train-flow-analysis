"""Tests of the training datasets, on a mocked S3 bucket.

The Gold partitions are synthetic: two trains a day, the first one late on
even days, the second one never seen in the real-time feed on day 3.
"""

import io
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest
from data_eng.utils.s3_connector import connect_to_s3
from delay_prediction.datasets import (
    FEATURE_COLUMNS,
    TARGET,
    build_dataset,
    label_features,
    list_versions,
    load_dataset,
    write_dataset,
)
from moto import mock_aws

from delay_prediction import datasets

BUCKET = "test-bucket"
FIRST_DAY = date(2026, 10, 1)
DAYS = 10


def put_parquet(client, key: str, frame: pl.DataFrame) -> None:
    """Write a frame as Parquet to the mocked bucket."""
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


def features_of(day: date) -> pl.DataFrame:
    """Features of the two trains of a day (the date is only in the folder)."""
    rows = []
    for trip_id in ("TRIP_A", "TRIP_B"):
        row = {"trip_id": trip_id, "train_number": trip_id, "route_id": "R1"}
        row["gtfs_version"] = day - timedelta(days=1)
        row |= {column: 1 for column in FEATURE_COLUMNS}
        rows.append(row)
    return pl.DataFrame(rows)


def trips_of(day: date, index: int) -> pl.DataFrame:
    """Trains of a day: TRIP_A late on even days, TRIP_B unobserved on day 3."""
    return pl.DataFrame(
        {
            "trip_id": ["TRIP_A", "TRIP_B"],
            "train_number": ["TRIP_A", "TRIP_B"],
            "is_observed": [True, index != 3],
            "is_cancelled": [False, False],
            "terminus_arrival_delay": [
                600 if index % 2 == 0 else 60,
                None if index == 3 else 0,
            ],
        }
    )


@pytest.fixture
def s3_client():
    """Provide a mocked bucket with DAYS days of features and trains.

    The features also have the next day, which has no trains yet (as the
    features of the current day in production).
    """
    with mock_aws():
        client = connect_to_s3(
            endpoint_url=None,
            access_key_id="testing",
            secret_access_key="testing",
            region_name="eu-west-1",
        )
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        for index in range(DAYS + 1):
            day = FIRST_DAY + timedelta(days=index)
            put_parquet(
                client,
                f"data/gold/ml_delay_features/date={day}/data_0.parquet",
                features_of(day),
            )
            if index < DAYS:
                put_parquet(
                    client,
                    f"data/gold/fact_train_trips/date={day}/data_0.parquet",
                    trips_of(day, index),
                )
        yield client


def build(client, **kwargs):
    """Build a dataset with a fixed build time."""
    return build_dataset(
        client,
        BUCKET,
        first_date=kwargs.pop("first_date", FIRST_DAY),
        valid_days=kwargs.pop("valid_days", 3),
        built_at=datetime(2026, 11, 26, 10, 15, tzinfo=UTC),
    )


def test_target_only_on_observed_trains(s3_client):
    """Unobserved trains and days without trains are left out."""
    version = build(s3_client)
    rows = pl.concat([version.train, version.valid])

    # 10 days x 2 trains, minus TRIP_B unobserved on day 3; the 11th day of
    # features has no trains
    assert rows.height == 19
    assert rows["date"].max() == FIRST_DAY + timedelta(days=DAYS - 1)
    # Only days with trains are read: 20 feature rows, 1 without a target
    assert version.manifest["dropped_rows"] == 1
    late = rows.filter(pl.col("trip_id") == "TRIP_A")[TARGET].to_list()
    assert late == [index % 2 == 0 for index in range(DAYS)]


def test_split_by_date_without_overlap(s3_client):
    """The last valid_days days are the valid set, the others the train set."""
    version = build(s3_client, valid_days=3)

    assert version.valid["date"].n_unique() == 3
    assert version.train["date"].max() < version.valid["date"].min()
    assert version.manifest["valid"]["first_date"] == "2026-10-08"


def test_first_date_filters_days(s3_client):
    """Days before first_date are not used."""
    version = build(s3_client, first_date=FIRST_DAY + timedelta(days=5))

    assert version.train["date"].min() == FIRST_DAY + timedelta(days=5)


def test_version_name_and_digest_are_stable(s3_client):
    """The same data built at the same time gives the same version."""
    first, second = build(s3_client), build(s3_client)

    assert first.name == second.name
    assert first.name.startswith("2026-11-26_101500_")
    assert first.name.endswith(first.manifest["digest"][:8])


def test_write_then_load_latest(s3_client, tmp_path):
    """A written version is listed, loaded from the bucket, then from the cache."""
    version = build(s3_client)
    write_dataset(s3_client, BUCKET, version)

    assert list_versions(s3_client, BUCKET) == [version.name]
    train, valid, manifest = load_dataset(
        "latest", s3_client=s3_client, bucket=BUCKET, cache_dir=tmp_path
    )
    assert train.equals(version.train)
    assert valid.equals(version.valid)
    assert manifest == version.manifest

    # Read from the cache: no client needed
    cached_train, _, _ = load_dataset(version.name, cache_dir=tmp_path)
    assert cached_train.equals(version.train)


def test_incomplete_version_is_not_listed(s3_client):
    """A version without its manifest (write interrupted) is ignored."""
    s3_client.put_object(
        Bucket=BUCKET, Key="data/ml/datasets/broken/train.parquet", Body=b"x"
    )

    assert list_versions(s3_client, BUCKET) == []


def test_trip_id_changed_matched_by_unique_train_number():
    """A train whose trip_id changed is matched by its number, when unique."""
    day = date(2026, 10, 13)
    features = pl.DataFrame(
        [
            {"date": day, "trip_id": trip_id, "train_number": number, "route_id": "R1"}
            | {"gtfs_version": day - timedelta(days=1)}
            | {column: 1 for column in FEATURE_COLUMNS}
            for trip_id, number in (("OLD_A", "NUM_A"), ("OLD_B", "NUM_B"))
        ]
    )
    trips = pl.DataFrame(
        {
            "date": [day] * 3,
            # NUM_A has a new trip_id; NUM_B is held by two trains of the day
            "trip_id": ["NEW_A", "NEW_B1", "NEW_B2"],
            "train_number": ["NUM_A", "NUM_B", "NUM_B"],
            "is_observed": [True] * 3,
            "is_cancelled": [False] * 3,
            "terminus_arrival_delay": [600, 0, 0],
        }
    )

    labelled = label_features(features, trips)

    assert labelled["trip_id"].to_list() == ["OLD_A"]
    assert labelled[TARGET].to_list() == [True]


def test_log_existing_version_reuses_the_written_version(s3_client, monkeypatch):
    """Logging a written version sends its own data and manifest to MLflow."""
    version = build(s3_client)
    write_dataset(s3_client, BUCKET, version)
    logged = {}

    def fake_log(dataset, source_uri):
        logged.update(dataset=dataset, source_uri=source_uri)
        return "run-1"

    monkeypatch.setattr(datasets, "log_dataset_to_mlflow", fake_log)

    run_id = datasets.log_existing_version("latest", s3_client=s3_client, bucket=BUCKET)

    assert run_id == "run-1"
    assert logged["dataset"].name == version.name
    assert logged["dataset"].train.equals(version.train)
    assert logged["dataset"].manifest == version.manifest
    assert logged["source_uri"] == f"s3://{BUCKET}/data/ml/datasets/{version.name}"
    # No new version written
    assert list_versions(s3_client, BUCKET) == [version.name]
