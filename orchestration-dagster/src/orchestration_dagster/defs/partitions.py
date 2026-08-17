import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-08-04", timezone="Europe/Paris"
)
