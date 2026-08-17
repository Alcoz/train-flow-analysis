{{ config(tags=["daily"]) }}

SELECT 
    route_id,
    AVG(trip_delay) as mean_delay,
FROM {{ ref("fact_train_trips") }} as ftt_1
WHERE arrival_time = (
    SELECT MAX(arrival_time)
    FROM {{ ref("fact_train_trips") }} as ftt_2
    WHERE ftt_1.trip_id = ftt_2.trip_id
)
GROUP BY route_id