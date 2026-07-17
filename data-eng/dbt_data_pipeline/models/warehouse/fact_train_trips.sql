SELECT
    trip_updates.trip_id,
    dim_trips.route_id,
    trip_updates.arrival_time, 
    trip_updates.arrival_delay as trip_delay
FROM {{ source("silver", "trip_updates")}} AS trip_updates
INNER JOIN {{ ref("dim_trips") }} as dim_trips on trip_updates.trip_id = dim_trips.trip_id