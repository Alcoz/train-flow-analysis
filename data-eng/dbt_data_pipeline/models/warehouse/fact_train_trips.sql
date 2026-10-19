{#-
    One row per train of the service date: every announced train
    (int_planned_trains) plus the trains added in real time. Holds what the
    delay and cancellation indicators need (see business_rules.md).
    Written to {external_root}/fact_train_trips/date=YYYY-MM-DD/data_0.parquet;
    overwrite_or_ignore only replaces the partition of this run, so the run must
    only emit rows for its own date.
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

-- The last snapshot of a train holds its whole journey: passed and skipped
-- stops stay in the list, and its last stop is the planned terminus.
WITH last_snapshot AS (
    SELECT *
    FROM {{ source("silver", "trip_updates") }}
    WHERE start_date = {{ run_date() }}
    QUALIFY fetched_at = max(fetched_at) OVER (PARTITION BY trip_id)
),

realtime AS (
    SELECT
        trip_id,
        NULLIF(regexp_extract(trip_id, '^[A-Z]+[0-9]+[A-Z]'), '') AS train_number,
        any_value(trip_schedule_relationship) AS realtime_status,
        -- Effective terminus: the last stop actually served
        arg_max(stop_id, stop_position)
            FILTER (WHERE stop_schedule_relationship <> 'SKIPPED') AS terminus_stop_id,
        arg_max(arrival_delay, stop_position)
            FILTER (WHERE stop_schedule_relationship <> 'SKIPPED') AS terminus_arrival_delay,
        arg_max(stop_schedule_relationship, stop_position) = 'SKIPPED' AS last_stop_skipped,
        max(fetched_at) AS last_fetched_at
    FROM last_snapshot
    GROUP BY trip_id
),

trains AS (
    SELECT
        coalesce(p.trip_id, r.trip_id) AS trip_id,
        coalesce(p.train_number, r.train_number) AS train_number,
        p.route_id,
        p.gtfs_status,
        r.* EXCLUDE (trip_id, train_number)
    FROM {{ ref("int_planned_trains") }} AS p
    FULL OUTER JOIN realtime AS r
        ON p.trip_id = r.trip_id
        -- Trains removed from the GTFS are sent with their short id
        -- (e.g. "OCESN19812F" and CANCELED): match them by train number
        OR (r.trip_id = r.train_number AND p.train_number = r.train_number)
    -- A train can be sent under both ids on the same day (e.g. OCESN9531F on
    -- 2026-10-06: full id until 18:08, then short id as ADDED): keep its
    -- latest real-time state, as last_snapshot does
    QUALIFY row_number() OVER (
        PARTITION BY coalesce(p.trip_id, r.trip_id)
        ORDER BY r.last_fetched_at DESC NULLS LAST
    ) = 1
)

SELECT
    {{ run_date() }} AS date,
    trip_id,
    train_number,
    -- Null for trains added in real time: the feed never sends route_id
    route_id,
    CASE
        WHEN gtfs_status IS NULL THEN 'added'
        WHEN realtime_status = 'CANCELED' THEN 'late_cancelled'
        -- Seen running: not cancelled, even if removed from the GTFS
        WHEN realtime_status IN ('SCHEDULED', 'ADDED') THEN 'planned'
        ELSE gtfs_status
    END AS train_status,
    gtfs_status,
    realtime_status,
    realtime_status IS NOT NULL AS is_observed,
    coalesce(
        realtime_status = 'CANCELED'
        OR (gtfs_status IN ('late_cancelled', 'deprogrammed') AND realtime_status IS NULL),
        false
    ) AS is_cancelled,
    -- Planned terminus skipped while the train ran
    coalesce(last_stop_skipped AND realtime_status <> 'CANCELED', false) AS is_limited,
    terminus_stop_id,
    -- Raw value in seconds: negative when early (indicators clip it to 0)
    terminus_arrival_delay,
    last_fetched_at
FROM trains
