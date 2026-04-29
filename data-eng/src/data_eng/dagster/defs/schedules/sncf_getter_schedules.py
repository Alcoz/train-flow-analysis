import dagster as dg

from data_eng.dagster.defs.jobs import sncf_getter_jobs

trip_update_schedule = dg.ScheduleDefinition(
    name="trip_update_schedule",
    cron_schedule="*/2 * * * *",
    job=sncf_getter_jobs.sncf_trip_updates_job,
)
