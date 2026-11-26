import dagster as dg
from orchestration_dagster.defs.jobs import sncf_transformations_jobs

# Build the previous day's partition at 03:00: trips started that day keep
# sending real-time updates after midnight (overnight trains).
sncf_delay_preparation_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_delay_preparation_job,
    hour_of_day=3,
    minute_of_hour=0,
)

sncf_alerts_preparation_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_alerts_preparation_job,
    hour_of_day=3,
    minute_of_hour=0,
)

# After the two jobs above, whose Gold files of the day it reads.
sncf_train_alerts_preparation_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_train_alerts_preparation_job,
    hour_of_day=3,
    minute_of_hour=30,
)

# After the jobs of 03:00 and 03:30, whose Gold files of D-1 it reads. Its
# partitions end on the current day: the run builds the features of today.
sncf_ml_features_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_ml_features_job,
    hour_of_day=4,
    minute_of_hour=0,
)

# After the 04:00 features: the champion predicts the trains of today.
sncf_ml_prediction_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_ml_prediction_job,
    hour_of_day=4,
    minute_of_hour=30,
)

# After the 03:00 delay job: the predictions of yesterday against the delays
# of yesterday.
sncf_ml_monitoring_schedule = dg.build_schedule_from_partitioned_job(
    sncf_transformations_jobs.sncf_ml_monitoring_job,
    hour_of_day=4,
    minute_of_hour=30,
)

# Every Monday after the 04:00 features: the dataset includes the trains of
# Sunday, built at 03:00.
sncf_ml_training_schedule = dg.ScheduleDefinition(
    job=sncf_transformations_jobs.sncf_ml_training_job,
    cron_schedule="0 5 * * 1",
    execution_timezone="Europe/Paris",
)
