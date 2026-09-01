import dagster as dg
from orchestration_dagster.defs.jobs import sncf_getter_jobs

theoretical_data_schedule = dg.build_schedule_from_partitioned_job(
    sncf_getter_jobs.sncf_theoretical_job,
    hour_of_day=9,
    minute_of_hour=0,
    name="theoretical_data_schedule",
)

trip_update_schedule = dg.build_schedule_from_partitioned_job(
    sncf_getter_jobs.sncf_trip_updates_job,
    hour_of_day=9,
    minute_of_hour=30,
    name="trip_update_schedule",
)

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
