import dagster as dg
from orchestration_dagster.defs.partitions import daily_partitions

sncf_medaillon_pipeline_job = dg.define_asset_job(
    name="sncf_data_preparation_job",
    selection=(
        dg.AssetSelection.key_prefixes("sncf_silver_theoretical_data")
        | dg.AssetSelection.assets("sncf_silver_continue_data")
        | dg.AssetSelection.assets("sncf_silver_alerts_data")
        | dg.AssetSelection.assets("dim_dates")
        | dg.AssetSelection.assets("dim_routes")
        | dg.AssetSelection.assets("dim_stops")
        | dg.AssetSelection.assets("dim_trips")
        | dg.AssetSelection.assets("int_planned_trains")
        | dg.AssetSelection.assets("alert_classification_rules")
        | dg.AssetSelection.assets("int_alerts")
        | dg.AssetSelection.assets("fact_train_trips")
        | dg.AssetSelection.assets("fact_trip_runs")
        | dg.AssetSelection.assets("agg_daily_route")
        | dg.AssetSelection.assets("fact_trip_disruptions")
    ),
    partitions_def=daily_partitions,
)
