import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-09-14", timezone="Europe/Paris"
)

# Service alerts are only collected since 2026-10-07: no bronze before that.
alerts_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-10-07", timezone="Europe/Paris"
)
