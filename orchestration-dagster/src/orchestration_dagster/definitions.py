from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():
    """Load and assemble all Dagster definitions from the defs folder."""
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
