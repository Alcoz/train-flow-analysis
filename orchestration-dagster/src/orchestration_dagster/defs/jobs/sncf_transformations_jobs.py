import dagster as dg
from orchestration_dagster.defs.partitions import (
    alerts_partitions,
    daily_partitions,
    ml_features_partitions,
)

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
        | dg.AssetSelection.assets("fact_train_trips")
        | dg.AssetSelection.assets("agg_daily_route")
    ),
    partitions_def=daily_partitions,
    # The steps depend on each other: one process for the whole run avoids
    # reloading the code location before each step.
    executor_def=dg.in_process_executor,
)

# Classified service alerts, since 2026-10-07 (start of their collection).
sncf_alerts_preparation_job = dg.define_asset_job(
    name="sncf_alerts_preparation_job",
    selection=(
        dg.AssetSelection.assets("sncf_silver_alerts_data")
        | dg.AssetSelection.assets("alert_classification_rules")
        | dg.AssetSelection.assets("dim_alerts")
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

# Delay time series for the delay prediction, since 2026-09-14. No schedule:
# launched by hand on the partitions wanted. Reads the Gold file of the day
# of fact_train_trips, so the delay job must have built that day first.
# int_planned_trains is not in Gold (in-memory DuckDB): rebuilt in the run.
# The getters fetch data that cannot be fetched later: these runs leave the
# queue after them (negative priority) and run one at a time (dagster.yaml).
sncf_time_series_delay_job = dg.define_asset_job(
    name="sncf_time_series_delay_job",
    selection=(
        dg.AssetSelection.assets("int_planned_trains")
        | dg.AssetSelection.assets("time_series_delay")
    ),
    partitions_def=daily_partitions,
    executor_def=dg.in_process_executor,
    tags={"dagster/priority": "-1", "time_series_delay": "true"},
)

# Features of the delay prediction for day D, since 2026-09-16. Reads the Gold
# files of fact_train_trips and fact_train_alerts up to D-1, so it runs after
# the delay and train alerts jobs of D-1.
sncf_ml_features_job = dg.define_asset_job(
    name="sncf_ml_features_job",
    selection=dg.AssetSelection.assets("ml_delay_features"),
    partitions_def=ml_features_partitions,
)

# Training loop of the delay prediction: a new dataset version, then the models
# trained on it, the best promoted to champion when it beats the baseline and
# the current champion. Scheduled every week.
sncf_ml_training_job = dg.define_asset_job(
    name="sncf_ml_training_job",
    selection=dg.AssetSelection.assets("ml_training_dataset", "ml_delay_model"),
)
