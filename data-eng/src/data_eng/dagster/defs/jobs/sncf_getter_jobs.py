import dagster as dg
from data_eng.dagster.defs.partitions import daily_partitions

sncf_theoretical_job = dg.define_asset_job(
    name="sncf_theoretical_job",
    selection=["sncf_bronze_theoretical_data"],
    partitions_def=daily_partitions,
)

sncf_trip_updates_job = dg.define_asset_job(
    name="sncf_trip_update_job",
    selection=["sncf_bronze_continue_data"],
    partitions_def=daily_partitions,
)
