SELECT
    route_id,
    trip_id
FROM {{ source("silver", "trips") }}