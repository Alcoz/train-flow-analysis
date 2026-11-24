{#-
    Delay time series for the delay prediction: one row per train and
    real-time snapshot within the planned running window of the train (from
    its first planned departure to its last planned arrival, stop_times).
    The delay is the forecast arrival delay at the effective terminus (last
    stop not skipped), as in fact_train_trips. The real-time feed only lists
    trains with something to report: a train absent from the snapshot, or
    sent without a delay, gets 0.
    Built by its own job, without schedule, after the delay job:
    fact_train_trips is read from its Gold file of the day (see the
    gold_partition macro), int_planned_trains is rebuilt in the run.
    Written to {external_root}/time_series_delay/date=YYYY-MM-DD/data_0.parquet;
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

-- depends_on: {{ ref("fact_train_trips") }}

{%- set lookback_versions = 30 %}

-- Announced trains not cancelled: added trains are left out, they have no
-- planned times in the GTFS
WITH trains AS (
    SELECT
        p.trip_id,
        p.train_number,
        p.last_gtfs_version
    FROM {{ ref("int_planned_trains") }} AS p
    INNER JOIN {{ gold_partition("fact_train_trips") }} AS f
        ON p.trip_id = f.trip_id
    WHERE NOT f.is_cancelled
),

-- First departure and last arrival in the last GTFS version announcing the
-- train. GTFS times are HH:MM:SS local times from midnight of the service
-- day (zero padded, so min and max work on the text) and can go past 24:00
planned_times AS (
    SELECT
        t.trip_id,
        min(st.departure_time) AS first_departure,
        max(st.arrival_time) AS last_arrival
    FROM {{ source("silver", "stop_times") }} AS st
    INNER JOIN trains AS t
        ON st.trip_id = t.trip_id
        AND {{ gtfs_version("st.filename") }} = t.last_gtfs_version
    -- Filtering on the file name only reads the files of these versions
    WHERE {{ gtfs_version("st.filename") }}
        BETWEEN {{ run_date() }} - {{ lookback_versions - 1 }} AND {{ run_date() }}
    GROUP BY t.trip_id
),

-- Converted to naive UTC, as the real-time times
windows AS (
    SELECT
        trip_id,
        timezone('UTC', timezone('Europe/Paris', {{ run_date() }} + to_seconds(
            split_part(first_departure, ':', 1)::INT * 3600
            + split_part(first_departure, ':', 2)::INT * 60
            + split_part(first_departure, ':', 3)::INT
        ))) AS window_start,
        timezone('UTC', timezone('Europe/Paris', {{ run_date() }} + to_seconds(
            split_part(last_arrival, ':', 1)::INT * 3600
            + split_part(last_arrival, ':', 2)::INT * 60
            + split_part(last_arrival, ':', 3)::INT
        ))) AS window_end
    FROM planned_times
),

feed AS (
    SELECT
        trip_id,
        NULLIF(regexp_extract(trip_id, '^[A-Z]+[0-9]+[A-Z]'), '') AS feed_train_number,
        stop_position,
        stop_schedule_relationship,
        arrival_delay,
        fetched_at
    FROM {{ source("silver", "trip_updates") }}
    WHERE start_date = {{ run_date() }}
),

snapshots AS (
    SELECT DISTINCT fetched_at
    FROM feed
),

-- Real-time id to GTFS trip_id. Trains removed from the GTFS are sent with
-- their short id (e.g. OCESN19812F): matched by train number, as in
-- fact_train_trips
id_map AS (
    SELECT
        f.trip_id AS feed_trip_id,
        t.trip_id,
        f.trip_id = t.trip_id AS is_full_id
    FROM (SELECT DISTINCT trip_id, feed_train_number FROM feed) AS f
    INNER JOIN trains AS t
        ON f.trip_id = t.trip_id
        OR (f.trip_id = f.feed_train_number AND t.train_number = f.trip_id)
),

-- Delay of each train in each snapshot it is in. A train sent under both
-- ids in one snapshot keeps its full id
observed AS (
    SELECT
        m.trip_id,
        u.fetched_at,
        arg_max(u.arrival_delay, u.stop_position)
            FILTER (WHERE u.stop_schedule_relationship <> 'SKIPPED') AS terminus_arrival_delay
    FROM feed AS u
    INNER JOIN id_map AS m
        ON u.trip_id = m.feed_trip_id
    GROUP BY m.trip_id, u.fetched_at, m.is_full_id
    QUALIFY row_number() OVER (
        PARTITION BY m.trip_id, u.fetched_at
        ORDER BY m.is_full_id DESC
    ) = 1
)

SELECT
    {{ run_date() }} AS date,
    w.trip_id,
    s.fetched_at,
    o.trip_id IS NOT NULL AS is_observed,
    coalesce(o.terminus_arrival_delay, 0)::BIGINT AS delay
FROM windows AS w
INNER JOIN snapshots AS s
    ON s.fetched_at BETWEEN w.window_start AND w.window_end
LEFT JOIN observed AS o
    ON w.trip_id = o.trip_id
    AND s.fetched_at = o.fetched_at
