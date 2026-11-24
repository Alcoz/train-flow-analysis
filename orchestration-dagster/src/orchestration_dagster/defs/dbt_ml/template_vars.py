import dagster as dg
from orchestration_dagster.defs.partitions import (
    ml_features_partitions as _ml_features_partitions,
)


@dg.template_var
def ml_features_partitions() -> dg.DailyPartitionsDefinition:
    """Return the daily partitions definition of the delay prediction features."""
    return _ml_features_partitions
