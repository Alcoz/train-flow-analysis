import dagster as dg
from orchestration_dagster.defs.partitions import (
    alerts_partitions as _alerts_partitions,
)


@dg.template_var
def alerts_partitions() -> dg.DailyPartitionsDefinition:
    """Return the daily partitions definition of the alert assets."""
    return _alerts_partitions
