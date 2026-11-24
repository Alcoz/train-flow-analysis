import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-09-14", timezone="Europe/Paris"
)

# Service alerts are only collected since 2026-10-07: no bronze before that.
alerts_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-10-07", timezone="Europe/Paris"
)

# Features of the delay prediction for day D, built on the morning of D from
# the data up to D-1: the partition of the current day exists (end_offset=1).
# Starts on 2026-09-16, the first day with a GTFS version the day before.
ml_features_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-09-16", timezone="Europe/Paris", end_offset=1
)
