import dagster as dg

sncf_theoretical_job = dg.define_asset_job(
    name="sncf_theoretical_job", selection=["sncf_theoretical_data"]
)

sncf_trip_updates_job = dg.define_asset_job(
    name="sncf_trip_update_job", selection=["sncf_continue_data"]
)
