"""Training datasets of the delay prediction: build, write, load.

A dataset version joins the features of ``gold/ml_delay_features`` (known
before day D) to the target of day D, from ``gold/fact_train_trips``, and
splits it by date: the last ``valid_days`` days are the validation set. Each
version is frozen under ``data/ml/datasets/<version>/`` on the bucket:

- ``train.parquet`` and ``valid.parquet``
- ``manifest.json``: date ranges, row counts, late rates, digest

Versions are named ``YYYY-MM-DD_HHMMSS_<digest>`` (UTC build time, then the
first characters of the data digest): sorting the names sorts them by build
time, and ``latest`` is the last one.

Log a version already on the bucket to MLflow (e.g. written while MLflow
was not configured):

    uv run --env-file <env file> python -m delay_prediction.datasets log <version>
"""

import argparse
import hashlib
import io
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
from data_eng.utils.s3_connector import (
    connect_to_s3,
    get_object_from_s3,
    send_object_to_s3,
)

GOLD_PREFIX = "data/gold"
DATASETS_PREFIX = "data/ml/datasets"

# A train is late when it reaches its effective terminus 5 minutes late or
# more, as in the features (see business_rules.md)
LATE_SECONDS = 300
TARGET = "is_late"
KEY_COLUMNS = ["date", "trip_id"]
# Identifiers kept in the dataset to trace a row, not to train on
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

# First day with 28 days of history in the features (fact_train_trips starts
# on 2026-09-15)
DEFAULT_FIRST_DATE = date(2026, 10, 13)
DEFAULT_VALID_DAYS = 14


@dataclass(frozen=True)
class DatasetVersion:
    """A built dataset version, ready to be written."""

    name: str
    train: pl.DataFrame
    valid: pl.DataFrame
    manifest: dict


def list_partition_dates(s3_client, bucket: str, table: str) -> list[date]:
    """Return the dates of the ``date=YYYY-MM-DD`` partitions of a Gold table."""
    paginator = s3_client.get_paginator("list_objects_v2")
    dates = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{GOLD_PREFIX}/{table}/"):
        for obj in page.get("Contents", []):
            folder = obj["Key"].split("/")[-2]
            if folder.startswith("date="):
                dates.add(date.fromisoformat(folder.removeprefix("date=")))
    return sorted(dates)


def read_gold_table(
    s3_client, bucket: str, table: str, dates: list[date]
) -> pl.DataFrame:
    """Read the partitions of a Gold table for the given dates.

    The partition column is not in the files: it is added from the folder.
    """
    frames = []
    for day in dates:
        prefix = f"{GOLD_PREFIX}/{table}/date={day.isoformat()}/"
        listing = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in listing.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                body = get_object_from_s3(s3_client, bucket, obj["Key"])
                frame = pl.read_parquet(io.BytesIO(body))
                frames.append(
                    frame.drop("date", strict=False).with_columns(
                        pl.lit(day).alias("date")
                    )
                )
    if not frames:
        raise ValueError(f"No partition of {table} found for the dates requested")
    return pl.concat(frames, how="diagonal_relaxed")


def match_trains(features: pl.DataFrame, trips: pl.DataFrame) -> pl.DataFrame:
    """Return the train of fact_train_trips of each feature row, when found.

    The features take the trip_id of the GTFS version of D-1, fact_train_trips
    the one of the last version announcing the train, which can be the version
    of D: the SNCF sometimes changes the trip_id of a train between versions.
    A row whose trip_id is not in fact_train_trips of D is matched by train
    number, when that number is held by a single train of D.
    """
    by_trip = features.select(*KEY_COLUMNS).join(trips, on=KEY_COLUMNS, how="inner")
    unique_numbers = (
        trips.filter(pl.col("train_number").is_not_null())
        .filter(pl.len().over("date", "train_number") == 1)
        # A train already matched by its trip_id is not matched twice
        .join(features.select(*KEY_COLUMNS), on=KEY_COLUMNS, how="anti")
    )
    by_number = (
        features.join(trips.select(*KEY_COLUMNS), on=KEY_COLUMNS, how="anti")
        .select(*KEY_COLUMNS, "train_number")
        .join(unique_numbers.drop("trip_id"), on=["date", "train_number"], how="inner")
    )
    return pl.concat(
        [by_trip, by_number.select(by_trip.columns)], how="vertical_relaxed"
    )


def label_features(features: pl.DataFrame, trips: pl.DataFrame) -> pl.DataFrame:
    """Join the target of day D to the features of day D.

    Trains are matched as in ``match_trains``. Only trains seen running, not
    cancelled and with a measured delay at the terminus are kept: the others
    have no observed answer. Early arrivals count as on time.
    """
    target = (
        match_trains(features, trips)
        .filter(
            pl.col("is_observed")
            & ~pl.col("is_cancelled")
            & pl.col("terminus_arrival_delay").is_not_null()
        )
        .select(
            *KEY_COLUMNS,
            (pl.col("terminus_arrival_delay") >= LATE_SECONDS).alias(TARGET),
        )
    )
    return features.join(target, on=KEY_COLUMNS, how="inner").select(
        *KEY_COLUMNS, *ID_COLUMNS, *FEATURE_COLUMNS, TARGET
    )


def split_by_date(
    frame: pl.DataFrame, valid_days: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split on dates: the last ``valid_days`` days are the validation set."""
    last_day = frame["date"].max()
    first_valid_day = last_day - timedelta(days=valid_days - 1)
    train = frame.filter(pl.col("date") < first_valid_day).sort(KEY_COLUMNS)
    valid = frame.filter(pl.col("date") >= first_valid_day).sort(KEY_COLUMNS)
    if train.is_empty() or valid.is_empty():
        raise ValueError("Not enough days to build both a train and a valid set")
    return train, valid


def to_parquet_bytes(frame: pl.DataFrame) -> bytes:
    """Write a frame to Parquet in memory."""
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def dataset_digest(train: pl.DataFrame, valid: pl.DataFrame) -> str:
    """Return a SHA-256 digest of the content of both sets."""
    digest = hashlib.sha256()
    for frame in (train, valid):
        digest.update(to_parquet_bytes(frame))
    return digest.hexdigest()


def _summary(frame: pl.DataFrame) -> dict:
    return {
        "first_date": frame["date"].min().isoformat(),
        "last_date": frame["date"].max().isoformat(),
        "days": frame["date"].n_unique(),
        "rows": frame.height,
        "late_rate": round(frame[TARGET].mean(), 4),
    }


def build_dataset(
    s3_client,
    bucket: str,
    first_date: date = DEFAULT_FIRST_DATE,
    valid_days: int = DEFAULT_VALID_DAYS,
    built_at: datetime | None = None,
) -> DatasetVersion:
    """Build a dataset version from the Gold features and trains.

    Uses every day from ``first_date`` that has both its features and its
    trains: the features of the current day have no target yet.
    """
    built_at = built_at or datetime.now(UTC)
    feature_dates = set(list_partition_dates(s3_client, bucket, "ml_delay_features"))
    trip_dates = set(list_partition_dates(s3_client, bucket, "fact_train_trips"))
    dates = sorted(d for d in feature_dates & trip_dates if d >= first_date)
    if not dates:
        raise ValueError(f"No day with both features and trains since {first_date}")

    features = read_gold_table(s3_client, bucket, "ml_delay_features", dates)
    trips = read_gold_table(s3_client, bucket, "fact_train_trips", dates)
    labelled = label_features(features, trips)
    train, valid = split_by_date(labelled, valid_days)

    digest = dataset_digest(train, valid)
    name = f"{built_at:%Y-%m-%d_%H%M%S}_{digest[:8]}"
    manifest = {
        "version": name,
        "built_at": built_at.isoformat(),
        "digest": digest,
        "source": {
            "features": f"{GOLD_PREFIX}/ml_delay_features",
            "target": f"{GOLD_PREFIX}/fact_train_trips",
        },
        "target": {
            "column": TARGET,
            "rule": f"terminus_arrival_delay >= {LATE_SECONDS} seconds",
            "kept": "trains observed, not cancelled, with a measured delay",
        },
        "feature_columns": FEATURE_COLUMNS,
        "id_columns": KEY_COLUMNS + ID_COLUMNS,
        "feature_rows": features.height,
        "labelled_rows": labelled.height,
        "dropped_rows": features.height - labelled.height,
        "valid_days": valid_days,
        "train": _summary(train),
        "valid": _summary(valid),
    }
    return DatasetVersion(name=name, train=train, valid=valid, manifest=manifest)


def write_dataset(s3_client, bucket: str, version: DatasetVersion) -> str:
    """Write a dataset version to the bucket and return its prefix."""
    prefix = f"{DATASETS_PREFIX}/{version.name}"
    send_object_to_s3(
        s3_client, bucket, to_parquet_bytes(version.train), f"{prefix}/train.parquet"
    )
    send_object_to_s3(
        s3_client, bucket, to_parquet_bytes(version.valid), f"{prefix}/valid.parquet"
    )
    # Written last: a version with a manifest is complete
    send_object_to_s3(
        s3_client,
        bucket,
        json.dumps(version.manifest, indent=2).encode(),
        f"{prefix}/manifest.json",
    )
    return prefix


def list_versions(s3_client, bucket: str) -> list[str]:
    """Return the complete dataset versions on the bucket, oldest first."""
    paginator = s3_client.get_paginator("list_objects_v2")
    versions = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{DATASETS_PREFIX}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/manifest.json"):
                versions.add(obj["Key"].split("/")[-2])
    return sorted(versions)


def s3_client_from_env():
    """Build an S3 client and return it with the bucket, from the S3_* variables."""
    client = connect_to_s3(
        endpoint_url=os.environ["S3_API"],
        access_key_id=os.environ["S3_ACCESS_KEY"],
        secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.environ["REGION_NAME"],
    )
    return client, os.environ["BUCKET_NAME"]


def load_dataset(
    version: str = "latest",
    s3_client=None,
    bucket: str | None = None,
    cache_dir: str | Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    """Load the train set, valid set and manifest of a dataset version.

    Without a client, one is built from the S3_* environment variables. With
    a ``cache_dir``, the version is read from ``<cache_dir>/<version>/`` when
    it is there, and copied there otherwise: a version never changes, so the
    cache is never stale. ``latest`` always asks the bucket which version is
    the last one.
    """
    cache = Path(cache_dir) if cache_dir is not None else None
    if version != "latest" and cache is not None:
        cached = cache / version
        if (cached / "manifest.json").exists():
            return _read_local(cached)

    if s3_client is None:
        s3_client, bucket = s3_client_from_env()
    if version == "latest":
        versions = list_versions(s3_client, bucket)
        if not versions:
            raise ValueError("No dataset version on the bucket")
        version = versions[-1]
        if cache is not None and (cache / version / "manifest.json").exists():
            return _read_local(cache / version)

    prefix = f"{DATASETS_PREFIX}/{version}"
    files = {
        name: get_object_from_s3(s3_client, bucket, f"{prefix}/{name}")
        for name in ("train.parquet", "valid.parquet", "manifest.json")
    }
    if cache is not None:
        target = cache / version
        target.mkdir(parents=True, exist_ok=True)
        # manifest.json last, as on the bucket: it marks a complete copy
        for name in ("train.parquet", "valid.parquet", "manifest.json"):
            (target / name).write_bytes(files[name])

    return (
        pl.read_parquet(io.BytesIO(files["train.parquet"])),
        pl.read_parquet(io.BytesIO(files["valid.parquet"])),
        json.loads(files["manifest.json"]),
    )


def _read_local(folder: Path) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    return (
        pl.read_parquet(folder / "train.parquet"),
        pl.read_parquet(folder / "valid.parquet"),
        json.loads((folder / "manifest.json").read_text()),
    )


def log_dataset_to_mlflow(version: DatasetVersion, source_uri: str) -> str:
    """Log a dataset version as a MLflow run and return the run id.

    The run is tagged ``purpose=training_dataset``: only metadata goes to
    MLflow, the data stays on the bucket at ``source_uri``. The tracking
    server and experiment are read from the environment (see .env.example).
    """
    import mlflow

    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])
    with mlflow.start_run(run_name=f"dataset_{version.name}") as run:
        mlflow.set_tags(
            {"purpose": "training_dataset", "dataset_version": version.name}
        )
        mlflow.log_params(
            {
                "dataset_version": version.name,
                "digest": version.manifest["digest"],
                "valid_days": version.manifest["valid_days"],
                "train_first_date": version.manifest["train"]["first_date"],
                "train_last_date": version.manifest["train"]["last_date"],
                "valid_first_date": version.manifest["valid"]["first_date"],
                "valid_last_date": version.manifest["valid"]["last_date"],
            }
        )
        mlflow.log_metrics(
            {
                "train_rows": version.manifest["train"]["rows"],
                "valid_rows": version.manifest["valid"]["rows"],
                "train_late_rate": version.manifest["train"]["late_rate"],
                "valid_late_rate": version.manifest["valid"]["late_rate"],
                "dropped_rows": version.manifest["dropped_rows"],
            }
        )
        for split, frame in (("train", version.train), ("valid", version.valid)):
            dataset = mlflow.data.from_polars(
                frame,
                source=f"{source_uri}/{split}.parquet",
                targets=TARGET,
                name=f"{version.name}_{split}",
            )
            mlflow.log_input(dataset, context=split)
        mlflow.log_dict(version.manifest, "manifest.json")
    return run.info.run_id


def log_existing_version(
    version: str, s3_client=None, bucket: str | None = None
) -> str:
    """Log a dataset version already on the bucket to MLflow, return the run id.

    Logs the same metadata as when the version was built, without writing a
    new version.
    """
    if s3_client is None:
        s3_client, bucket = s3_client_from_env()
    if version == "latest":
        version = list_versions(s3_client, bucket)[-1]
    train, valid, manifest = load_dataset(version, s3_client=s3_client, bucket=bucket)
    dataset = DatasetVersion(name=version, train=train, valid=valid, manifest=manifest)
    return log_dataset_to_mlflow(dataset, f"s3://{bucket}/{DATASETS_PREFIX}/{version}")


def main() -> None:
    """Command line of the datasets: list the versions or log one to MLflow."""
    parser = argparse.ArgumentParser(description="Training datasets on the bucket")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List the complete versions, oldest first")
    log = commands.add_parser("log", help="Log a version to MLflow")
    log.add_argument("version", help="Version name, or latest")
    args = parser.parse_args()

    if args.command == "list":
        s3_client, bucket = s3_client_from_env()
        print("\n".join(list_versions(s3_client, bucket)))
    elif args.command == "log":
        run_id = log_existing_version(args.version)
        print(f"Version {args.version} logged to MLflow: run {run_id}")


if __name__ == "__main__":
    main()
