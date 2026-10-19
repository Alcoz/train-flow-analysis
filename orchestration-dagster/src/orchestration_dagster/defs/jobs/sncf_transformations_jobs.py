import dagster as dg
from orchestration_dagster.defs.partitions import alerts_partitions, daily_partitions

# A job has a single partitions definition and the DuckDB of each dbt run is
# in memory: each job builds what it needs, and only shares Gold files.

# Delay and cancellation indicators (1 to 4), since 2026-09-14.
sncf_delay_preparation_job = dg.define_asset_job(
    name="sncf_delay_preparation_job",
    selection=(
        dg.AssetSelection.key_prefixes("sncf_silver_theoretical_data")
        | dg.AssetSelection.assets("sncf_silver_continue_data")
        | dg.AssetSelection.assets("dim_dates")
        | dg.AssetSelection.assets("dim_routes")
        | dg.AssetSelection.assets("int_planned_trains")
        | dg.AssetSelection.assets("fact_trip_runs")
        | dg.AssetSelection.assets("agg_daily_route")
    ),
    partitions_def=daily_partitions,
)

# Classified service alerts, since 2026-10-07 (start of their collection).
sncf_alerts_preparation_job = dg.define_asset_job(
    name="sncf_alerts_preparation_job",
    selection=(
        dg.AssetSelection.assets("sncf_silver_alerts_data")
        | dg.AssetSelection.assets("alert_classification_rules")
        | dg.AssetSelection.assets("fact_alerts")
    ),
    partitions_def=alerts_partitions,
)

# Alerts linked to trains (indicators 5 to 7): reads the Gold files of the
# day written by the two jobs above, so it runs after them.
sncf_train_alerts_preparation_job = dg.define_asset_job(
    name="sncf_train_alerts_preparation_job",
    selection=dg.AssetSelection.assets("fact_train_alerts"),
    partitions_def=alerts_partitions,
)
