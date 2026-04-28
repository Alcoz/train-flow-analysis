import dagster as dg

sncf_getter_job = dg.define_asset_job(
    name="sncf_getter_job", selection=["sncf_theoretical_data"]
)
