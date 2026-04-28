from dagster import Definitions, load_assets_from_modules

from data_eng.dagster.defs.assets import sncf_getter_assets
from data_eng.dagster.defs.jobs import sncf_getter_jobs

defs = Definitions(
    assets=load_assets_from_modules([sncf_getter_assets]),
    jobs=[sncf_getter_jobs.sncf_getter_job],
)
