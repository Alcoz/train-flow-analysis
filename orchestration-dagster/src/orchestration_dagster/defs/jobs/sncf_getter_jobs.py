import dagster as dg
from orchestration_dagster.defs.partitions import daily_partitions

sncf_theoretical_job = dg.define_asset_job(
    name="sncf_theoretical_job",
    selection=(
        dg.AssetSelection.assets("sncf_bronze_theoretical_data")
        | dg.AssetSelection.key_prefixes("sncf_silver_theoretical_data")
    ),
    partitions_def=daily_partitions,
)

sncf_trip_updates_job = dg.define_asset_job(
    name="sncf_trip_update_job",
    selection=["sncf_bronze_continue_data", "sncf_silver_continue_data"],
    partitions_def=daily_partitions,
)

sncf_data_preparation_job = dg.define_asset_job(
    name="sncf_data_preparation_job",
    selection=(
        dg.AssetSelection.assets("sncf_bronze_theoretical_data")
        | dg.AssetSelection.key_prefixes("sncf_silver_theoretical_data")
        | dg.AssetSelection.assets("sncf_bronze_continue_data")
        | dg.AssetSelection.assets("sncf_silver_continue_data")
        | dg.AssetSelection.assets("dim_routes")
        | dg.AssetSelection.assets("dim_trips")
        | dg.AssetSelection.assets("fact_train_trips")
        # | dg.AssetSelection.assets("train_delay")
    ),
    partitions_def=daily_partitions,
)

theoretical_getter_job = dg.define_asset_job(
    name="theoretical_getter_job",
    selection=(dg.AssetSelection.assets("sncf_bronze_theoretical_data")),
)

trip_update_getter_job = dg.define_asset_job(
    name="trip_update_getter_job",
    selection=(dg.AssetSelection.assets("sncf_bronze_continue_data")),
)
