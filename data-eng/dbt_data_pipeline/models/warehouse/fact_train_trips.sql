{#-
    Written to {external_root}/fact_train_trips/date_id=YYYY-MM-DD/data_0.parquet.
    overwrite_or_ignore only replaces the partitions written by this run
    ('overwrite' would wipe every other date), so each run must only emit
    rows for its own date: see the start_date filter below.
-#}
{{
    config(
        materialized='external',
        options={
            "partition_by": "date_id",
            "overwrite_or_ignore": True
        },
        tags=["daily"]
    )
}}

WITH snapshots_agg AS (
    SELECT
        trip_id,
        stop_id,
        start_date,
        MIN(departure_delay)            AS min_departure_delay,
        MAX(departure_delay)            AS max_departure_delay,
        MIN(arrival_delay)              AS min_arrival_delay,
        MAX(arrival_delay)              AS max_arrival_delay,
        -- Last known delays: the latest forecast is the closest to what happened
        arg_max(departure_delay, fetched_at)
            FILTER (WHERE departure_delay IS NOT NULL) AS last_departure_delay,
        arg_max(arrival_delay, fetched_at)
            FILTER (WHERE arrival_delay IS NOT NULL)   AS last_arrival_delay,
        MIN(fetched_at)                 AS first_fetched_at,
        MAX(fetched_at)                 AS last_fetched_at,
    FROM {{ source("silver", "trip_updates") }}
    -- Silver is already stored by service date; this guards the partition
    -- overwrite: rows of another date would replace that date's data.
    WHERE start_date = {{ run_date() }}
    GROUP BY trip_id, stop_id, start_date
)

SELECT
    s.trip_id,
    r.route_id,
    s.stop_id,
    s.start_date AS date_id,
    s.min_departure_delay,
    s.max_departure_delay,
    s.min_arrival_delay,
    s.max_arrival_delay,
    s.last_departure_delay,
    s.last_arrival_delay,
    s.first_fetched_at,
    s.last_fetched_at,
    t.snapshot_date
FROM snapshots_agg AS s
-- Join the dimension snapshot of the run date; snapshot_date records which
-- version each fact row was built with.
INNER JOIN {{ ref("dim_trips") }}  AS t
    ON s.trip_id = t.trip_id
    AND t.snapshot_date = {{ run_date() }}
INNER JOIN {{ ref("dim_routes") }} AS r
    ON t.route_id = r.route_id
    AND r.snapshot_date = t.snapshot_date
