"""Materialization of ml_training_dataset on a mocked S3 bucket.

Synthetic Gold partitions: one train a day, late on even days.
"""

import io
from datetime import date, timedelta

import dagster as dg
import polars as pl
import pytest
from delay_prediction.datasets import FEATURE_COLUMNS, list_versions
from moto import mock_aws
from orchestration_dagster.defs.assets.ml_dataset_assets import ml_training_dataset
from orchestration_dagster.defs.resources import S3_Resource

BUCKET = "test-bucket"
FIRST_DAY = date(2026, 10, 13)


def put_parquet(client, key: str, frame: pl.DataFrame) -> None:
    """Write a frame as Parquet to the mocked bucket."""
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


@pytest.fixture
def s3_resource(monkeypatch):
    """Provide an S3 resource on a mocked bucket with 6 days of features and trains."""
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    with mock_aws():
        resource = S3_Resource(
            s3_api="https://s3.eu-west-1.amazonaws.com",
            s3_access_key="testing",
            s3_secret_access_key="testing",
            region_name="eu-west-1",
            bucket_name=BUCKET,
        )
        client = resource.get_client()
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        for index in range(6):
            day = FIRST_DAY + timedelta(days=index)
            features = pl.DataFrame(
                [
                    {"trip_id": "TRIP_A", "train_number": "TRIP_A", "route_id": "R1"}
                    | {"gtfs_version": day - timedelta(days=1)}
                    | {column: 1 for column in FEATURE_COLUMNS}
                ]
            )
            trips = pl.DataFrame(
                {
                    "trip_id": ["TRIP_A"],
                    "train_number": ["TRIP_A"],
                    "is_observed": [True],
                    "is_cancelled": [False],
                    "terminus_arrival_delay": [600 if index % 2 == 0 else 0],
                }
            )
            put_parquet(
                client,
                f"data/gold/ml_delay_features/date={day}/data_0.parquet",
                features,
            )
            put_parquet(
                client, f"data/gold/fact_train_trips/date={day}/data_0.parquet", trips
            )
        yield resource


def test_materialization_writes_a_version(s3_resource):
    """A run writes one complete version and reports it in its metadata."""
    result = dg.materialize(
        [ml_training_dataset],
        resources={"s3_resource": s3_resource},
        run_config={
            "ops": {"ml_training_dataset": {"config": {"valid_days": 2}}},
        },
    )

    assert result.success
    metadata = result.asset_materializations_for_node("ml_training_dataset")[0].metadata
    versions = list_versions(s3_resource.get_client(), BUCKET)
    assert versions == [metadata["version"].value]
    assert metadata["train_rows"].value == 4
    assert metadata["valid_rows"].value == 2
    assert metadata["mlflow_run_id"].value.startswith("not logged")
