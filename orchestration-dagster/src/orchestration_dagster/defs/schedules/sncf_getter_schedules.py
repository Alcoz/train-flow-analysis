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
