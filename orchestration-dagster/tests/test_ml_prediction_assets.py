"""Materialization of ml_delay_monitoring, monitoring replaced by a stub."""

import dagster as dg
import pytest
from orchestration_dagster.defs.assets import ml_prediction_assets
from orchestration_dagster.defs.resources import S3_Resource

S3_RESOURCE = S3_Resource(
    s3_api="https://s3.eu-west-1.amazonaws.com",
    s3_access_key="testing",
    s3_secret_access_key="testing",
    region_name="eu-west-1",
    bucket_name="test-bucket",
)


def materialize_monitoring(monkeypatch, failed: list[str]) -> list[str]:
    """Materialize the monitoring of 2026-11-25 with these failed tests; return the Discord messages."""
    monkeypatch.setenv("MLFLOW_REGISTERED_MODEL_NAME", "delay_prediction")
    monkeypatch.setattr(
        ml_prediction_assets,
        "monitor_day",
        lambda *args: {"champion_version": "1", "trains": 10, "failed_tests": failed},
    )
    messages = []
    monkeypatch.setattr(ml_prediction_assets, "post_to_discord", messages.append)
    result = dg.materialize(
        [ml_prediction_assets.ml_delay_monitoring],
        partition_key="2026-11-25",
        resources={"s3_resource": S3_RESOURCE},
    )
    assert result.success
    return messages


@pytest.mark.parametrize(
    ("failed", "alerts"), [([], 0), (["F1 score: 0.40 < 0.46"], 1)]
)
def test_discord_is_alerted_only_on_failed_tests(monkeypatch, failed, alerts):
    """Failed Evidently tests send one Discord message that lists them."""
    messages = materialize_monitoring(monkeypatch, failed)

    assert len(messages) == alerts
    assert all("F1 score: 0.40 < 0.46" in message for message in messages)
