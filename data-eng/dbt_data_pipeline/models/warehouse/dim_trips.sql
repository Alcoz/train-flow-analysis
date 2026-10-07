{#-
    One snapshot of the theoretical trips per day, written to
    {external_root}/dim_trips/date=YYYY-MM-DD/. Rebuilding a date
    only replaces that date's partition.
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
