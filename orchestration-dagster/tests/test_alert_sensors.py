import dagster as dg
import pytest
import requests
from orchestration_dagster.defs.sensors import alert_sensors


@dg.op
def broken_op():
    """Fail like a getter whose upstream API is down."""
    raise ValueError("SNCF API unreachable")


@dg.job
def broken_job():
    """Job that always fails."""
    broken_op()


@dg.op
def working_op():
    """Succeed without doing anything."""


@dg.job(name="broken_job")
def fixed_job():
    """Run `broken_job` again once its failure is fixed."""
    working_op()


@pytest.fixture
def discord_messages(monkeypatch) -> list[str]:
    """Capture Discord messages instead of sending them."""
    messages: list[str] = []
    monkeypatch.setattr(alert_sensors, "post_to_discord", messages.append)
    return messages


def _evaluate_failure_sensor(instance: dg.DagsterInstance) -> None:
    result = broken_job.execute_in_process(instance=instance, raise_on_error=False)
    context = dg.build_run_status_sensor_context(
        sensor_name="discord_on_run_failure",
        dagster_event=result.get_run_failure_event(),
        dagster_instance=instance,
        dagster_run=result.dagster_run,
    ).for_run_failure()
    alert_sensors.discord_on_run_failure(context)


def _evaluate_recovery_sensor(instance: dg.DagsterInstance) -> None:
    result = fixed_job.execute_in_process(instance=instance)
    context = dg.build_run_status_sensor_context(
        sensor_name="discord_on_run_recovery",
        dagster_event=result.get_run_success_event(),
        dagster_instance=instance,
        dagster_run=result.dagster_run,
    )
    alert_sensors.discord_on_run_recovery(context)


def test_failure_alert_contains_job_and_error(discord_messages):
    """The alert names the job and carries the step error."""
    with dg.instance_for_test() as instance:
        _evaluate_failure_sensor(instance)

    assert len(discord_messages) == 1
    assert "broken_job" in discord_messages[0]
    assert "SNCF API unreachable" in discord_messages[0]


def test_failure_streak_alerts_once(discord_messages):
    """Consecutive failures of the same job only alert on the first one."""
    with dg.instance_for_test() as instance:
        for _ in range(3):
            _evaluate_failure_sensor(instance)

    assert len(discord_messages) == 1


def test_recovery_alerts_only_after_failure(discord_messages):
    """A success is reported only when it ends a failure streak."""
    with dg.instance_for_test() as instance:
        _evaluate_recovery_sensor(instance)
        assert discord_messages == []

        _evaluate_failure_sensor(instance)
        _evaluate_recovery_sensor(instance)

    assert len(discord_messages) == 2
    assert "fonctionne à nouveau" in discord_messages[1]


def test_post_to_discord_without_webhook_is_noop(monkeypatch):
    """Local runs without a webhook configured send nothing."""
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(alert_sensors.requests, "post", pytest.fail)

    alert_sensors.post_to_discord("hello")


@pytest.fixture
def healthchecks_pings(monkeypatch) -> list[str]:
    """Capture Healthchecks pings instead of sending them."""
    pings: list[str] = []

    def fake_get(url, params, timeout):
        pings.append(url)
        response = requests.Response()
        response.status_code = 200
        response._content = b"OK"
        return response

    monkeypatch.setenv("HEALTHCHECK_PING_KEY", "test-key")
    monkeypatch.setattr(alert_sensors.requests, "get", fake_get)
    return pings


def _evaluate_heartbeat_sensor(instance: dg.DagsterInstance, tags: dict) -> None:
    result = fixed_job.execute_in_process(instance=instance, tags=tags)
    context = dg.build_run_status_sensor_context(
        sensor_name="healthchecks_heartbeat",
        dagster_event=result.get_run_success_event(),
        dagster_instance=instance,
        dagster_run=result.dagster_run,
    )
    alert_sensors.healthchecks_heartbeat(context)


def test_heartbeat_pings_the_job_check_on_scheduled_runs(healthchecks_pings):
    """Each scheduled job pings a check named after it."""
    with dg.instance_for_test() as instance:
        _evaluate_heartbeat_sensor(
            instance, tags={"dagster/schedule_name": "broken_job_schedule"}
        )

    assert healthchecks_pings == ["https://hc-ping.com/test-key/broken_job"]


def test_heartbeat_ignores_manual_runs(healthchecks_pings):
    """A manual run says nothing about the schedule being alive."""
    with dg.instance_for_test() as instance:
        _evaluate_heartbeat_sensor(instance, tags={})

    assert healthchecks_pings == []
