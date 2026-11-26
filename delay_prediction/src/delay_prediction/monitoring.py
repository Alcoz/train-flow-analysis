"""Monitoring of the predictions of a day with Evidently, once their trains have run.

The predictions of day D are compared, the day after, to whether each train
was late (gold/fact_train_trips of D). The Evidently report holds:

- the drift of each feature, against the training set of the champion that
  made the predictions;
- the precision, recall and F1 of the predictions.

Two tests give the alerts: the F1 of the day is not more than MAX_TEST_F1_DROP
below the test F1 of the champion, and less than half of the features drift.
The two reports (drift, performance) are runs of the Evidently workspace
EVIDENTLY_WORKSPACE (a folder or an s3:// path), seen in the Evidently UI.
"""

import os
from datetime import date, datetime

import mlflow
import polars as pl
from evidently import BinaryClassification, DataDefinition, Dataset, Report
from evidently.core.report import Snapshot
from evidently.metrics import DriftedColumnsCount, F1Score
from evidently.presets import ClassificationPreset, DataDriftPreset
from evidently.tests import gte, lt
from evidently.ui.workspace import Workspace
from mlflow import MlflowClient

from delay_prediction.datasets import (
    CATEGORICAL_FEATURE_COLUMNS,
    FEATURE_COLUMNS,
    KEY_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    TARGET,
    add_target,
    load_dataset,
)
from delay_prediction.predictions import (
    CHAMPION_VERSION,
    PREDICTION,
    read_gold_partition,
    read_predictions,
)
from delay_prediction.registry import MAX_TEST_F1_DROP

EVIDENTLY_PROJECT = "delay_prediction"
MAX_DRIFTED_FEATURES_SHARE = 0.5

FEATURES_DEFINITION = DataDefinition(
    numerical_columns=NUMERIC_FEATURE_COLUMNS,
    categorical_columns=CATEGORICAL_FEATURE_COLUMNS,
)
CLASSIFICATION_DEFINITION = DataDefinition(
    classification=[BinaryClassification(target=TARGET, prediction_labels=PREDICTION)]
)


def with_target(predictions: pl.DataFrame, trains: pl.DataFrame) -> pl.DataFrame:
    """Return the predictions of the trains that ran, with whether they were late."""
    return add_target(predictions, trains).join(
        predictions.select(*KEY_COLUMNS, PREDICTION), on=KEY_COLUMNS
    )


def as_dataset(trains: pl.DataFrame, data_definition: DataDefinition) -> Dataset:
    """Return the trains as an Evidently dataset."""
    return Dataset.from_pandas(trains.to_pandas(), data_definition=data_definition)


def monitoring_reports(
    reference: pl.DataFrame, current: pl.DataFrame, min_f1: float, day: date
) -> list[Snapshot]:
    """Return the Evidently reports of the predictions of a day: drift, then performance.

    The training set has no predictions: it is the reference of the drift only.
    """
    # Only the tests given here: the default ones would test every column
    drift_report = Report(
        [
            DataDriftPreset(columns=FEATURE_COLUMNS),
            DriftedColumnsCount(
                columns=FEATURE_COLUMNS, share_tests=[lt(MAX_DRIFTED_FEATURES_SHARE)]
            ),
        ],
        include_tests=False,
    )
    performance_report = Report(
        [ClassificationPreset(), F1Score(tests=[gte(min_f1)])], include_tests=False
    )
    timestamp = datetime.combine(day, datetime.min.time())
    drift = drift_report.run(
        current_data=as_dataset(current.select(FEATURE_COLUMNS), FEATURES_DEFINITION),
        reference_data=as_dataset(
            reference.select(FEATURE_COLUMNS), FEATURES_DEFINITION
        ),
        timestamp=timestamp,
        name=f"{day} drift",
    )
    performance = performance_report.run(
        current_data=as_dataset(
            current.select(TARGET, PREDICTION), CLASSIFICATION_DEFINITION
        ),
        timestamp=timestamp,
        name=f"{day} performance",
    )
    return [drift, performance]


def failed_tests(snapshots: list[Snapshot]) -> list[str]:
    """Return the description of the failed tests of the reports."""
    return [
        test["description"]
        for snapshot in snapshots
        for test in snapshot.dict()["tests"]
        if test["status"] == "FAIL"
    ]


def save_reports(snapshots: list[Snapshot]) -> None:
    """Add the reports to the Evidently project of the delay prediction."""
    workspace = Workspace(os.environ["EVIDENTLY_WORKSPACE"])
    projects = workspace.search_project(EVIDENTLY_PROJECT)
    project = projects[0] if projects else workspace.create_project(EVIDENTLY_PROJECT)
    for snapshot in snapshots:
        workspace.add_run(project.id, snapshot)


def champion_reference(model_name: str, version: str, s3_client, bucket: str):
    """Return the training set and the minimum F1 of a champion version."""
    model_uri = f"models:/{model_name}/{version}"
    training_dataset = mlflow.models.get_model_info(model_uri).metadata[
        "training_dataset"
    ]
    splits, _ = load_dataset(training_dataset, s3_client, bucket)
    run_id = MlflowClient().get_model_version(model_name, version).run_id
    test_f1 = MlflowClient().get_run(run_id).data.metrics["test_f1"]
    return splits["train"], test_f1 - MAX_TEST_F1_DROP


def monitor_day(s3_client, bucket: str, model_name: str, day: date) -> dict:
    """Monitor the predictions of a day and save the report; return its summary."""
    predictions = read_predictions(s3_client, bucket, day)
    trains = read_gold_partition(s3_client, bucket, "fact_train_trips", day)
    current = with_target(predictions, trains)
    version = predictions[CHAMPION_VERSION][0]
    reference, min_f1 = champion_reference(model_name, version, s3_client, bucket)
    snapshots = monitoring_reports(reference, current, min_f1, day)
    save_reports(snapshots)
    return {
        CHAMPION_VERSION: version,
        "trains": current.height,
        "failed_tests": failed_tests(snapshots),
    }
