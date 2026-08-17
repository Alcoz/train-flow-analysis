{{ config(tags=["daily"]) }}

SELECT
    trip_id,
    route_id,
    direction_id
FROM {{ source("silver", "trips") }}