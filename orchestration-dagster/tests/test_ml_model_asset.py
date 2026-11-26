"""Materialization of ml_delay_model on a mocked S3 bucket, training replaced by a stub."""

import dagster as dg
import polars as pl
import pytest
from delay_prediction.datasets import SPLIT_NAMES, write_dataset
from moto import mock_aws
from orchestration_dagster.defs.assets.ml_model_assets import ml_delay_model
from orchestration_dagster.defs.resources import S3_Resource

BUCKET = "test-bucket"


@pytest.fixture
def bucket_with_dataset():
    """Provide an S3 resource on a mocked bucket holding one dataset version, and the version."""
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
        splits = {name: pl.DataFrame({"is_late": [True]}) for name in SPLIT_NAMES}
        yield resource, write_dataset(client, BUCKET, splits)


def test_materialization_trains_on_the_latest_dataset(bucket_with_dataset, monkeypatch):
    """The models are trained on the latest version; the decision is in the metadata."""
    s3_resource, version = bucket_with_dataset
    trained_on = []

    def train_and_promote(splits, dataset_version):
        trained_on.append(dataset_version)
        best = {"model": "logistic_regression", "f1": 0.48, "test_f1": 0.47}
        return None, best | {"run_id": "run-1"}, "promoted: version 1"

    monkeypatch.setattr(
        "orchestration_dagster.defs.assets.ml_model_assets.train_and_promote",
        train_and_promote,
    )

    result = dg.materialize([ml_delay_model], resources={"s3_resource": s3_resource})

    assert result.success
    assert trained_on == [version]
    metadata = result.asset_materializations_for_node("ml_delay_model")[0].metadata
    assert metadata["dataset_version"].value == version
    assert metadata["best_model"].value == "logistic_regression"
    assert metadata["promotion"].value == "promoted: version 1"
