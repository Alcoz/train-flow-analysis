{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

{% if is_incremental() %}

WITH last_recuperation AS (
    SELECT
        COALESCE(
            MAX(recuperation_date),
            TIMESTAMP '1900-01-01 00:00:00'
        ) AS max_recuperation_date
    FROM {{ this }}
)

SELECT
    trip_updates.trip_id,
    dim_trips.route_id,
    trip_updates.arrival_time,
    trip_updates.arrival_delay AS trip_delay,
    trip_updates.recuperation_date
FROM {{ source("silver", "trip_updates") }} AS trip_updates
INNER JOIN {{ ref("dim_trips") }} AS dim_trips
    ON trip_updates.trip_id = dim_trips.trip_id
CROSS JOIN last_recuperation
WHERE trip_updates.recuperation_date >= last_recuperation.max_recuperation_date

{% else %}

SELECT
    trip_updates.trip_id,
    dim_trips.route_id,
    trip_updates.arrival_time,
    trip_updates.arrival_delay AS trip_delay,
    trip_updates.recuperation_date
FROM {{ source("silver", "trip_updates") }} AS trip_updates
INNER JOIN {{ ref("dim_trips") }} AS dim_trips
    ON trip_updates.trip_id = dim_trips.trip_id

{% endif %}