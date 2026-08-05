from dagster import Definitions, load_assets_from_modules

from data_eng.dagster.defs.assets import sncf_getter_assets
from data_eng.dagster.defs.jobs import sncf_getter_jobs
from data_eng.dagster.defs.schedules import sncf_getter_schedules

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
)
