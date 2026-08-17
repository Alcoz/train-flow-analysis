{{
    config(
        materialized='external',
        location="s3://sncf-bucket/data/gold/continue",
        options={
            "partition_by": '"date"',
            "overwrite": True,
            "filename_pattern": "'fact_train_trips_{i}'"
        },
        tags=["daily"]
    )
}}

SELECT
    trip_updates.trip_id,
    dim_trips.route_id,
    trip_updates.arrival_time,
    trip_updates.arrival_delay AS trip_delay,
    trip_updates.recuperation_date,
    cast('{{ var("today") }}' as date) as "date"
FROM {{ source("silver", "trip_updates") }} AS trip_updates
INNER JOIN {{ ref("dim_trips") }} AS dim_trips
    ON trip_updates.trip_id = dim_trips.trip_id