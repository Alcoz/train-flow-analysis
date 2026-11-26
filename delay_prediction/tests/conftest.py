"""Fixtures shared by the tests of the delay prediction."""

import mlflow
import pytest


@pytest.fixture
def local_mlflow(tmp_path, monkeypatch):
    """Point MLflow tracking and registry to a temporary SQLite database."""
    monkeypatch.delenv("MLFLOW_REGISTRY_URI", raising=False)
    database = f"sqlite:///{tmp_path}/mlflow.db"
    mlflow.set_tracking_uri(database)
    mlflow.set_registry_uri(database)
    mlflow.set_experiment(
        experiment_id=mlflow.create_experiment(
            "test", (tmp_path / "artifacts").as_uri()
        )
    )
