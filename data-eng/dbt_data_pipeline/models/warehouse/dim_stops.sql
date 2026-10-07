{#-
    One snapshot of the theoretical stops per day, written to
    {external_root}/dim_stops/date=YYYY-MM-DD/. Rebuilding a date
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
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    {{ run_date() }} AS date
FROM {{ source("silver", "stops") }}
