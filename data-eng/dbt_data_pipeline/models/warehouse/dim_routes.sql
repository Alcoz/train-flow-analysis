{#-
    One snapshot of the theoretical routes per day, written to
    {external_root}/dim_routes/snapshot_date=YYYY-MM-DD/. Rebuilding a date
    only replaces that date's partition.
-#}
{{
    config(
        materialized='external',
        options={
            "partition_by": "snapshot_date",
            "overwrite_or_ignore": True
        },
        tags=["daily"]
    )
}}

SELECT
    route_id,
    route_short_name,
    route_long_name,
    -- route_long_name is free text: 'A - B', 'A - B - C' (via B) or 'A B'.
    -- Terminals are only known when the ' - ' separator is used.
    CASE WHEN contains(route_long_name, ' - ')
        THEN string_split(route_long_name, ' - ')[1]
    END AS terminal_station_1,
    CASE WHEN contains(route_long_name, ' - ')
        THEN string_split(route_long_name, ' - ')[-1]
    END AS terminal_station_2,
    {{ run_date() }} AS snapshot_date
FROM {{ source("silver", "routes")}}
