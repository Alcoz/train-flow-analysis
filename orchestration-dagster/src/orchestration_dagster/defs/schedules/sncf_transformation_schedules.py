import dagster as dg
from orchestration_dagster.defs.jobs import sncf_transformations_jobs

# Builds the previous day's partition at 03:00: trips started that day keep
# sending real-time updates after midnight (overnight trains).
sncf_data_preparation_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_medaillon_pipeline_job,
    hour_of_day=3,
    minute_of_hour=0,
)
