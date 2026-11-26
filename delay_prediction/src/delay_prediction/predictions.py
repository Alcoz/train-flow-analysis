"""Daily prediction of the delay prediction champion.

On the morning of day D, the champion of the Unity Catalog registry predicts
which trains of D arrive late, from the features of D (gold/ml_delay_features).
The predictions are written to data/ml/predictions/date=D/, with the version
of the champion that made them.
"""

import io
from datetime import date

import mlflow
import polars as pl
from data_eng.utils.s3_connector import get_object_from_s3, send_object_to_s3
from mlflow import MlflowClient

from delay_prediction.datasets import (
    FEATURE_COLUMNS,
    GOLD_PREFIX,
    ID_COLUMNS,
    KEY_COLUMNS,
    list_keys,
)
from delay_prediction.models import features
from delay_prediction.registry import CHAMPION_ALIAS

PREDICTIONS_PREFIX = "data/ml/predictions"
PREDICTION = "prediction"
CHAMPION_VERSION = "champion_version"


def read_gold_partition(s3_client, bucket: str, table: str, day: date) -> pl.DataFrame:
    """Read the partition of a day of a Gold table, with its date."""
    keys = list_keys(s3_client, bucket, f"{GOLD_PREFIX}/{table}/date={day}/")
    partition = pl.concat(
        [pl.read_parquet(get_object_from_s3(s3_client, bucket, key)) for key in keys],
        how="diagonal_relaxed",
    )
    return partition.with_columns(date=pl.lit(day))


def champion_version(model_name: str) -> str:
    """Return the version of the registered model holding the champion alias."""
    # A string in Unity Catalog, an int in other registries
    champion = MlflowClient().get_model_version_by_alias(model_name, CHAMPION_ALIAS)
    return str(champion.version)


def predict(model, day_features: pl.DataFrame, version: str) -> pl.DataFrame:
    """Return the trains with their features, their prediction and the champion version."""
    return day_features.select(
        *KEY_COLUMNS, *ID_COLUMNS, *FEATURE_COLUMNS
    ).with_columns(
        pl.Series(PREDICTION, model.predict(features(day_features))),
        pl.lit(version).alias(CHAMPION_VERSION),
    )


def predictions_key(day: date) -> str:
    """Return the key of the predictions of a day."""
    return f"{PREDICTIONS_PREFIX}/date={day}/predictions.parquet"


def predict_day(s3_client, bucket: str, model_name: str, day: date) -> pl.DataFrame:
    """Predict the trains of a day with the current champion and write the predictions."""
    version = champion_version(model_name)
    model = mlflow.sklearn.load_model(f"models:/{model_name}/{version}")
    day_features = read_gold_partition(s3_client, bucket, "ml_delay_features", day)
    predictions = predict(model, day_features, version)
    buffer = io.BytesIO()
    predictions.write_parquet(buffer)
    send_object_to_s3(s3_client, bucket, buffer.getvalue(), predictions_key(day))
    return predictions


def read_predictions(s3_client, bucket: str, day: date) -> pl.DataFrame:
    """Read the predictions of a day."""
    return pl.read_parquet(get_object_from_s3(s3_client, bucket, predictions_key(day)))
