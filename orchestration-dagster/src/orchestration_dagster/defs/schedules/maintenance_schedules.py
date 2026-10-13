import dagster as dg
from orchestration_dagster.defs.jobs import maintenance_jobs

# Runs after the 03:00 medallion pipeline.
purge_old_runs_schedule = dg.ScheduleDefinition(
    job=maintenance_jobs.purge_old_runs_job,
    cron_schedule="0 4 * * *",
    execution_timezone="Europe/Paris",
)
