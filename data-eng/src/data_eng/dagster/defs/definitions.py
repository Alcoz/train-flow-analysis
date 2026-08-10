from dagster import Definitions, load_assets_from_modules, EnvVar

from data_eng.dagster.defs.assets import sncf_getter_assets
from data_eng.dagster.defs.jobs import sncf_getter_jobs
from data_eng.dagster.defs.schedules import sncf_getter_schedules
from data_eng.dagster.defs.resources import S3_Resource

defs = Definitions(
    assets=load_assets_from_modules([sncf_getter_assets]),
    jobs=[
        sncf_getter_jobs.sncf_theoretical_job,
        sncf_getter_jobs.sncf_trip_updates_job,
    ],
    schedules=[
        sncf_getter_schedules.theoretical_data_schedule,
        sncf_getter_schedules.trip_update_schedule,
    ],
    resources={
        "s3_resource": S3_Resource(
            s3_api=EnvVar("S3_API"),
            s3_access_key=EnvVar("S3_ACCESS_KEY"),
            s3_secret_access_key=EnvVar("S3_SECRET_ACCESS_KEY"),
            region_name=EnvVar("REGION_NAME"),
            bucket_name=EnvVar("BUCKET_NAME"),
        )
    },
)
