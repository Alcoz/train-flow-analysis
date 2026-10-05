import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-09-14", timezone="Europe/Paris"
)
