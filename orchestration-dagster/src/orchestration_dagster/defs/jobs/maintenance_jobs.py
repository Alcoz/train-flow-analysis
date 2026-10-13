from datetime import UTC, datetime, timedelta

import dagster as dg

# Dagster OSS never purges runs: each one keeps its row, its event log and its
# compute logs forever. The getter schedules launch ~1000 runs a day, so only
# those are purged; daily partitioned runs stay to keep the partition history.
HIGH_FREQUENCY_JOBS = ["trip_update_getter_job", "service_alerts_getter_job"]

FINISHED_STATUSES = [
    dg.DagsterRunStatus.SUCCESS,
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
]


class PurgeRunsConfig(dg.Config):
    """Which jobs to purge and how many days of runs to keep."""

    job_names: list[str] = HIGH_FREQUENCY_JOBS
    retention_days: int = 14
    batch_size: int = 500


@dg.op
def purge_old_runs(context: dg.OpExecutionContext, config: PurgeRunsConfig) -> int:
    """Delete finished runs older than the retention window, with their events and compute logs."""
    instance = context.instance
    created_before = datetime.now(UTC) - timedelta(days=config.retention_days)
    deleted = 0

    for job_name in config.job_names:
        runs_filter = dg.RunsFilter(
            job_name=job_name,
            statuses=FINISHED_STATUSES,
            created_before=created_before,
        )
        while run_ids := instance.get_run_ids(
            filters=runs_filter, limit=config.batch_size
        ):
            for run_id in run_ids:
                instance.delete_run(run_id)
                # delete_run only cleans the database; stdout/stderr files stay on disk
                instance.compute_log_manager.delete_logs(prefix=[run_id])
            deleted += len(run_ids)
        context.log.info(
            f"{job_name}: runs created before {created_before:%Y-%m-%d %H:%M} purged"
        )

    context.log.info(f"{deleted} runs deleted")
    return deleted


@dg.job
def purge_old_runs_job():
    """Keep the Dagster database and compute logs from growing without bound."""
    purge_old_runs()
