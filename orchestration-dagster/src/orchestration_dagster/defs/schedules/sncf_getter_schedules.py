import dagster as dg
from orchestration_dagster.defs.jobs import sncf_getter_jobs

theoretical_getter_schedule = dg.ScheduleDefinition(
    job=sncf_getter_jobs.theoretical_getter_job,
    cron_schedule="30 10 * * *",
    execution_timezone="Europe/Paris",
)

trip_update_getter_schedule = dg.ScheduleDefinition(
    job=sncf_getter_jobs.trip_update_getter_job,
    cron_schedule="*/2 * * * *",
    execution_timezone="Europe/Paris",
)
