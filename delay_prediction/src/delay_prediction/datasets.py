"""Training datasets of the delay prediction.

A dataset joins the features of each day (gold/ml_delay_features) to whether
the train was late that day (gold/fact_train_trips), then splits it by date
into a train, validation and test set (the last days). Each version is written to
data/ml/datasets/<version>/ on the bucket and logged to MLflow.
"""

import io
import os
from datetime import UTC, date, datetime, timedelta

import mlflow
import polars as pl
from data_eng.utils.s3_connector import (
    connect_to_s3,
    get_object_from_s3,
    send_object_to_s3,
)

GOLD_PREFIX = "data/gold"
DATASETS_PREFIX = "data/ml/datasets"

# A train is late when it reaches its terminus 5 minutes late or more
LATE_DELAY_SECONDS = 300
TARGET = "is_late"
KEY_COLUMNS = ["date", "trip_id"]
ID_COLUMNS = ["train_number", "route_id", "gtfs_version"]
FEATURE_COLUMNS = [
    "day_of_week",
    "departure_hour",
    "planned_duration_minutes",
    "planned_stops",
    "train_runs_28d",
    "train_late_rate_28d",
    "train_late_rate_7d",
    "train_mean_delay_28d",
    "train_cancel_rate_28d",
    "train_last_delay",
    "train_days_since_last_run",
    "route_late_rate_28d",
    "route_late_rate_7d",
    "route_late_rate_1d",
    "route_mean_delay_7d",
    "network_late_rate_1d",
    "train_alerts_1d",
    "route_alerts_1d",
]

# First day with 28 days of history in the features
DEFAULT_FIRST_DATE = date(2026, 10, 13)
DEFAULT_VALID_DAYS = 14
DEFAULT_TEST_DAYS = 7
# Written in this order: a version without its test set is incomplete
SPLIT_NAMES = ("train", "valid", "test")


def s3_client_from_env():
    """Return an S3 client and the bucket name, from the S3_* variables."""
    s3_client = connect_to_s3(
        endpoint_url=os.environ["S3_API"],
        access_key_id=os.environ["S3_ACCESS_KEY"],
        secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.environ["REGION_NAME"],
    )
    return s3_client, os.environ["BUCKET_NAME"]


def list_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    """Return every object key under a prefix."""
    pages = s3_client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    )
    return [obj["Key"] for page in pages for obj in page.get("Contents", [])]


def read_gold_table(s3_client, bucket: str, table: str, first_date: date):
    """Read the partitions of a Gold table from first_date, with their date.

    The partition date is not in the files: it comes from the date=YYYY-MM-DD
    folder.
    """
    partitions = []
    for key in list_keys(s3_client, bucket, f"{GOLD_PREFIX}/{table}/"):
        partition_date = date.fromisoformat(key.split("date=")[1].split("/")[0])
        if partition_date >= first_date:
            partition = pl.read_parquet(get_object_from_s3(s3_client, bucket, key))
            partitions.append(partition.with_columns(date=pl.lit(partition_date)))
    return pl.concat(partitions, how="diagonal_relaxed")


def attach_trains(features: pl.DataFrame, trains: pl.DataFrame) -> pl.DataFrame:
    """Attach to each feature row its train of the same day in fact_train_trips.

    Matched by trip_id. The SNCF sometimes changes the trip_id of a train
    between two GTFS versions: such a row is matched by train number, when a
    single train of the day holds that number.
    """
    matched_by_trip_id = features.join(
        trains.drop("train_number"), on=KEY_COLUMNS, how="inner"
    )
    single_trains = (
        trains.filter(pl.len().over("date", "train_number") == 1)
        .join(features, on=KEY_COLUMNS, how="anti")
        .drop("trip_id")
    )
    matched_by_number = features.join(trains, on=KEY_COLUMNS, how="anti").join(
        single_trains, on=["date", "train_number"], how="inner"
    )
    return pl.concat([matched_by_trip_id, matched_by_number], how="diagonal_relaxed")


def add_target(features: pl.DataFrame, trains: pl.DataFrame) -> pl.DataFrame:
    """Keep the trains seen running, not cancelled, with a measured delay, and add their target."""
    trains = trains.select(
        *KEY_COLUMNS,
        "train_number",
        "is_observed",
        "is_cancelled",
        "terminus_arrival_delay",
    )
    return (
        attach_trains(features, trains)
        .filter(
            pl.col("is_observed")
            & ~pl.col("is_cancelled")
            & pl.col("terminus_arrival_delay").is_not_null()
        )
        .with_columns(
            (pl.col("terminus_arrival_delay") >= LATE_DELAY_SECONDS).alias(TARGET)
        )
        .select(*KEY_COLUMNS, *ID_COLUMNS, *FEATURE_COLUMNS, TARGET)
    )


def split_by_date(dataset: pl.DataFrame, valid_days: int, test_days: int) -> dict:
    """Split on dates: the last test_days days are the test set, the valid_days before the validation set."""
    first_test_date = dataset["date"].max() - timedelta(days=test_days - 1)
    first_valid_date = first_test_date - timedelta(days=valid_days)
    split_of_each_row = (
        pl.when(pl.col("date") >= first_test_date)
        .then(pl.lit("test"))
        .when(pl.col("date") >= first_valid_date)
        .then(pl.lit("valid"))
        .otherwise(pl.lit("train"))
    )
    dataset = dataset.with_columns(split=split_of_each_row).sort(KEY_COLUMNS)
    return {
        split_name: dataset.filter(pl.col("split") == split_name).drop("split")
        for split_name in SPLIT_NAMES
    }


def build_dataset(
    s3_client, bucket: str, first_date: date, valid_days: int, test_days: int
) -> dict:
    """Build the train, validation and test sets from the Gold tables.

    The features of the current day have no train yet: the join leaves them out.
    """
    features = read_gold_table(s3_client, bucket, "ml_delay_features", first_date)
    trains = read_gold_table(s3_client, bucket, "fact_train_trips", first_date)
    return split_by_date(add_target(features, trains), valid_days, test_days)


def write_dataset(s3_client, bucket: str, splits: dict) -> str:
    """Write a new dataset version to the bucket and return its name."""
    version = f"{datetime.now(UTC):%Y-%m-%d_%H%M%S}"
    for split_name in SPLIT_NAMES:
        buffer = io.BytesIO()
        splits[split_name].write_parquet(buffer)
        key = f"{DATASETS_PREFIX}/{version}/{split_name}.parquet"
        send_object_to_s3(s3_client, bucket, buffer.getvalue(), key)
    return version


def latest_version(s3_client, bucket: str) -> str:
    """Return the name of the last complete dataset version."""
    keys = list_keys(s3_client, bucket, f"{DATASETS_PREFIX}/")
    return max(key.split("/")[-2] for key in keys if key.endswith("/test.parquet"))


def load_dataset(version: str = "latest", s3_client=None, bucket: str | None = None):
    """Return the train, validation and test sets and the version name of a dataset.

    Without a client, one is built from the S3_* variables.
    """
    if s3_client is None:
        s3_client, bucket = s3_client_from_env()
    if version == "latest":
        version = latest_version(s3_client, bucket)
    splits = {
        split_name: pl.read_parquet(
            get_object_from_s3(
                s3_client, bucket, f"{DATASETS_PREFIX}/{version}/{split_name}.parquet"
            )
        )
        for split_name in SPLIT_NAMES
    }
    return splits, version


def log_dataset_inputs(splits: dict, version: str, bucket: str) -> None:
    """Log the sets as inputs of the active MLflow run.

    MLflow stores their schema, size and digest; the data stays on the bucket.
    """
    for split_name, split in splits.items():
        dataset = mlflow.data.from_polars(
            split,
            source=f"s3://{bucket}/{DATASETS_PREFIX}/{version}/{split_name}.parquet",
            targets=TARGET,
            name=f"{version}_{split_name}",
        )
        mlflow.log_input(dataset, context=split_name)


def log_dataset_to_mlflow(splits: dict, version: str, bucket: str) -> str:
    """Log a dataset version as a MLflow run and return its id."""
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])
    with mlflow.start_run(run_name=f"dataset_{version}") as run:
        mlflow.set_tags({"purpose": "training_dataset", "dataset_version": version})
        mlflow.log_params(
            {
                f"{split_name}_dates": f"{split['date'].min()} -> {split['date'].max()}"
                for split_name, split in splits.items()
            }
        )
        log_dataset_inputs(splits, version, bucket)
    return run.info.run_id
