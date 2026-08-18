# template_vars.py
import dagster as dg
from orchestration_dagster.defs.partitions import (
    daily_partitions as _daily_partitions,
)  # adapte le chemin d'import réel


@dg.template_var
def daily_partitions() -> dg.DailyPartitionsDefinition:
    """Return the daily partitions definition shared across SNCF assets."""
    return _daily_partitions
