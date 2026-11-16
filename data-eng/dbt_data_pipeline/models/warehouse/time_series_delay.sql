{#-
    Delay time series for the delay prediction: one row per train and
    real-time snapshot while the train runs. The stop of a row is the next
    stop the train has not passed yet: it moves to the following stop once
    the train has left it.
    The real-time feed only lists trains with something to report, so every
    snapshot of the running window gets a row, even without the train:
    - in the feed: delay sent for the current stop;
    - absent: last delay known for the stop when it was above 5 minutes,
      otherwise 0 (a delay of 5 minutes or less is assumed caught up).
    A stop is passed when its departure time is at or before the snapshot:
    feed time when the train is in the snapshot, otherwise planned time of
    stop_times plus the delay above. The terminus uses its arrival time.
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
{%- set catchable_delay_seconds = 300 %}

-- Announced trains not cancelled: added trains are left out, they have no
-- planned stops in the GTFS
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

-- GTFS times are local times counted from midnight of the service day and
-- can go past 24:00:00: converted to naive UTC, as the real-time times
planned_stop_times AS (
    SELECT
        trip_id,
        stop_id,
        stop_sequence,
        {{ gtfs_version() }} AS gtfs_version,
        split_part(arrival_time, ':', 1)::INT * 3600
            + split_part(arrival_time, ':', 2)::INT * 60
            + split_part(arrival_time, ':', 3)::INT AS arrival_seconds,
        split_part(departure_time, ':', 1)::INT * 3600
            + split_part(departure_time, ':', 2)::INT * 60
            + split_part(departure_time, ':', 3)::INT AS departure_seconds
    FROM {{ source("silver", "stop_times") }}
    -- Filtering on the file name only reads the files of these versions
    WHERE {{ gtfs_version() }}
        BETWEEN {{ run_date() }} - {{ lookback_versions - 1 }} AND {{ run_date() }}
),

-- Stops of each train in the last GTFS version announcing it
planned_stops AS (
    SELECT
        t.trip_id,
        st.stop_id,
        st.stop_sequence,
        timezone('UTC', timezone(
            'Europe/Paris', {{ run_date() }} + to_seconds(st.arrival_seconds)
        )) AS planned_arrival,
        timezone('UTC', timezone(
            'Europe/Paris', {{ run_date() }} + to_seconds(st.departure_seconds)
        )) AS planned_departure,
        st.stop_sequence = max(st.stop_sequence) OVER (PARTITION BY t.trip_id) AS is_terminus
    FROM planned_stop_times AS st
    INNER JOIN trains AS t
        ON st.trip_id = t.trip_id
        AND st.gtfs_version = t.last_gtfs_version
),

feed AS (
    SELECT
        *,
        NULLIF(regexp_extract(trip_id, '^[A-Z]+[0-9]+[A-Z]'), '') AS feed_train_number
    FROM {{ source("silver", "trip_updates") }}
    WHERE start_date = {{ run_date() }}
),

-- Real-time id to GTFS trip_id. Trains removed from the GTFS are sent with
-- their short id (e.g. OCESN19812F): matched by train number, as in
-- fact_train_trips
id_map AS (
    SELECT
        f.trip_id AS feed_trip_id,
        t.trip_id
    FROM (SELECT DISTINCT trip_id, feed_train_number FROM feed) AS f
    INNER JOIN trains AS t
        ON f.trip_id = t.trip_id
        OR (f.trip_id = f.feed_train_number AND t.train_number = f.trip_id)
),

-- One row per train, snapshot and stop sent by the feed. A train sent under
-- both ids in one snapshot keeps its full id
feed_stops AS (
    SELECT
        m.trip_id,
        u.fetched_at,
        u.stop_id,
        u.stop_schedule_relationship,
        -- Delay at the stop: arrival, departure at the origin (no arrival)
        coalesce(u.arrival_delay, u.departure_delay) AS delay_raw,
        coalesce(u.departure_time, u.arrival_time) AS feed_departure,
        coalesce(u.arrival_time, u.departure_time) AS feed_arrival
    FROM feed AS u
    INNER JOIN id_map AS m
        ON u.trip_id = m.feed_trip_id
    QUALIFY row_number() OVER (
        PARTITION BY m.trip_id, u.fetched_at, u.stop_id
        ORDER BY (u.trip_id = m.trip_id) DESC, u.stop_position
    ) = 1
),

snapshots AS (
    SELECT DISTINCT fetched_at
    FROM feed
),

-- Running window: from the planned departure at the origin to the latest
-- arrival, planned or sent by the feed (a late train runs past its plan)
windows AS (
    SELECT
        p.trip_id,
        p.window_start,
        greatest(p.planned_end, coalesce(f.feed_end, p.planned_end)) AS window_end
    FROM (
        SELECT
            trip_id,
            min(planned_departure) AS window_start,
            max(planned_arrival) AS planned_end
        FROM planned_stops
        GROUP BY trip_id
    ) AS p
    LEFT JOIN (
        SELECT
            trip_id,
            max(feed_arrival) AS feed_end
        FROM feed_stops
        GROUP BY trip_id
    ) AS f
        ON p.trip_id = f.trip_id
),

train_snapshots AS (
    SELECT
        w.trip_id,
        s.fetched_at,
        o.trip_id IS NOT NULL AS is_observed
    FROM windows AS w
    INNER JOIN snapshots AS s
        ON s.fetched_at BETWEEN w.window_start AND w.window_end
    LEFT JOIN (SELECT DISTINCT trip_id, fetched_at FROM feed_stops) AS o
        ON w.trip_id = o.trip_id
        AND s.fetched_at = o.fetched_at
),

-- Every stop of the train at every snapshot of its running window
stop_grid AS (
    SELECT
        ts.trip_id,
        ts.fetched_at,
        ts.is_observed,
        ps.stop_id,
        ps.stop_sequence,
        ps.is_terminus,
        ps.planned_arrival,
        coalesce(ps.planned_departure, ps.planned_arrival) AS planned_departure,
        fs.delay_raw,
        fs.feed_departure,
        fs.feed_arrival,
        -- Last delay and status sent for this stop, up to this snapshot
        last_value(fs.delay_raw IGNORE NULLS) OVER stop_history AS last_known_delay,
        last_value(fs.stop_schedule_relationship IGNORE NULLS) OVER stop_history
            AS last_known_relationship
    FROM train_snapshots AS ts
    INNER JOIN planned_stops AS ps
        ON ts.trip_id = ps.trip_id
    LEFT JOIN feed_stops AS fs
        ON ts.trip_id = fs.trip_id
        AND ts.fetched_at = fs.fetched_at
        AND ps.stop_id = fs.stop_id
    WINDOW stop_history AS (
        PARTITION BY ts.trip_id, ps.stop_sequence
        ORDER BY ts.fetched_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

delays AS (
    SELECT
        *,
        coalesce(
            delay_raw,
            -- Not sent: catchable delays are assumed caught up
            CASE
                WHEN last_known_delay > {{ catchable_delay_seconds }} THEN last_known_delay
                ELSE 0
            END
        ) AS delay
    FROM stop_grid
),

passing AS (
    SELECT
        *,
        coalesce(last_known_relationship = 'SKIPPED', false)
            OR CASE
                WHEN is_terminus
                    THEN coalesce(feed_arrival, planned_arrival + to_seconds(delay))
                ELSE coalesce(feed_departure, planned_departure + to_seconds(delay))
            END <= fetched_at AS is_passed
    FROM delays
)

-- Next stop not passed; a train with every stop passed has arrived: no row
SELECT
    {{ run_date() }} AS date,
    trip_id,
    fetched_at,
    stop_id,
    stop_sequence,
    is_observed,
    delay_raw,
    delay
FROM passing
WHERE NOT is_passed
QUALIFY row_number() OVER (
    PARTITION BY trip_id, fetched_at
    ORDER BY stop_sequence
) = 1
