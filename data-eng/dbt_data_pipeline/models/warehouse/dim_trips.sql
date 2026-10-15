{#-
    One snapshot of the theoretical trips per day, written to
    {external_root}/dim_trips/date=YYYY-MM-DD/. Rebuilding a date
    only replaces the partition of that date.
-#}
{{
    config(
        materialized='external',
        options={
            "partition_by": "date",
            "overwrite_or_ignore": True
        },
        tags=["daily"]
    )
}}

SELECT
    trip_id,
    route_id,
    direction_id,
    {{ run_date() }} AS date
FROM {{ source("silver", "trips") }}
-- The source holds every GTFS version: keep the one of the run date
WHERE {{ gtfs_version() }} = {{ run_date() }}
