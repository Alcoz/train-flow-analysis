import os

import dagster as dg
import requests
from dagster_shared.error import SerializableErrorInfo
from orchestration_dagster.defs.jobs import sncf_getter_jobs
from orchestration_dagster.defs.jobs.maintenance_jobs import FINISHED_STATUSES

# The webserver is only reachable through the SSH tunnel.
DAGSTER_UI_URL = "http://localhost:3000"
DISCORD_DETAILS_LIMIT = 1500

# Alerting sensors start on their own: one left stopped by mistake would fail
# silently, which is exactly what they are here to prevent. They write nothing.


def post_to_discord(content: str) -> None:
    """Send a message to the Discord channel; no-op when no webhook is configured."""
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return
    response = requests.post(webhook_url, json={"content": content}, timeout=10)
    response.raise_for_status()


def previous_run_status(
    instance: dg.DagsterInstance, run_id: str
) -> dg.DagsterRunStatus | None:
    """Return the status of the job's last finished run before `run_id`."""
    record = instance.get_run_record_by_id(run_id)
    previous = instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=record.dagster_run.job_name,
            statuses=FINISHED_STATUSES,
            created_before=record.create_timestamp,
        ),
        limit=1,
    )
    return previous[0].dagster_run.status if previous else None


def root_cause_message(error: SerializableErrorInfo) -> str:
    """Unwrap Dagster's step error to the exception the op actually raised."""
    while error.cause:
        error = error.cause
    return error.message.strip()


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING)
def discord_on_run_failure(context: dg.RunFailureSensorContext) -> None:
    """Alert on the first failure of a streak, so a job failing every 2 min alerts once."""
    run = context.dagster_run
    if previous_run_status(context.instance, run.run_id) == dg.DagsterRunStatus.FAILURE:
        return

    step_errors = [
        root_cause_message(event.event_specific_data.error)
        for event in context.get_step_failure_events()
        if event.event_specific_data.error
    ]
    details = step_errors[0] if step_errors else context.failure_event.message
    post_to_discord(
        f"🔴 **{run.job_name}** a échoué\n"
        f"```{details[:DISCORD_DETAILS_LIMIT]}```\n"
        f"{DAGSTER_UI_URL}/runs/{run.run_id}"
    )


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def discord_on_run_recovery(context: dg.RunStatusSensorContext) -> None:
    """Close the failure streak opened by `discord_on_run_failure`."""
    run = context.dagster_run
    if previous_run_status(context.instance, run.run_id) == dg.DagsterRunStatus.FAILURE:
        post_to_discord(f"🟢 **{run.job_name}** fonctionne à nouveau")


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[sncf_getter_jobs.trip_update_getter_job],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def healthchecks_heartbeat(context: dg.RunStatusSensorContext) -> None:
    """Ping Healthchecks.io after each successful trip update run.

    Healthchecks alerts Discord when pings stop, which covers what Dagster cannot
    report itself: database down, daemon stuck, VM off, schedule turned off.
    """
    ping_url = os.getenv("HEALTHCHECK_PING_URL")
    if ping_url:
        requests.get(ping_url, timeout=10).raise_for_status()
