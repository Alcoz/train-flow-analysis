import dagster as dg

theoretical_getter_job = dg.define_asset_job(
    name="theoretical_getter_job",
    selection=(dg.AssetSelection.assets("sncf_bronze_theoretical_data")),
)

trip_update_getter_job = dg.define_asset_job(
    name="trip_update_getter_job",
    selection=(dg.AssetSelection.assets("sncf_bronze_continue_data")),
)

service_alerts_getter_job = dg.define_asset_job(
    name="service_alerts_getter_job",
    selection=(dg.AssetSelection.assets("sncf_bronze_service_alerts_data")),
)
