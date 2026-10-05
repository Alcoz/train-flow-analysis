import dagster as dg
from orchestration_dagster.defs.partitions import daily_partitions

sncf_medaillon_pipeline_job = dg.define_asset_job(
    name="sncf_data_preparation_job",
    selection=(
        dg.AssetSelection.key_prefixes("sncf_silver_theoretical_data")
        | dg.AssetSelection.assets("sncf_silver_continue_data")
        | dg.AssetSelection.assets("dim_dates")
        | dg.AssetSelection.assets("dim_routes")
        | dg.AssetSelection.assets("dim_stops")
        | dg.AssetSelection.assets("dim_trips")
        | dg.AssetSelection.assets("fact_train_trips")
    ),
    partitions_def=daily_partitions,
)
