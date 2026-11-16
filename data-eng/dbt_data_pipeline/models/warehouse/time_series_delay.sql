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

    Memory: a grid of every stop at every snapshot gets the run killed.
    Stops are only crossed with the snapshots that contain the train. Between
    two of them the delays do not change, so each stop is the current one
    over a time interval, and the snapshots are joined on these intervals.
    DuckDB is also capped and spills to disk (pre_hook), and reset after.

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
        tags=["daily"],
        pre_hook=[
            "SET memory_limit = '" ~ var('time_series_memory_limit', '1GB') ~ "'",
            "SET threads = 2",
            "SET preserve_insertion_order = false",
            "SET temp_directory = '/tmp/duckdb_spill'"
        ],
        post_hook=[
            "RESET memory_limit",
            "RESET threads",
            "RESET preserve_insertion_order"
        ]
    )
}}

-- depends_on: {{ ref("fact_train_trips") }}

{%- set lookback_versions = 30 %}
{%- set catchable_delay_seconds = 300 %}

{#- Delay kept for a stop when the train is absent: catchable delays are caught up -#}
{%- set absent_delay -%}
    CASE WHEN last_known_delay > {{ catchable_delay_seconds }} THEN last_known_delay ELSE 0 END
{%- endset %}

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

-- Stops of each train in the last GTFS version announcing it. GTFS times are
-- local times counted from midnight of the service day and can go past
-- 24:00:00: converted to naive UTC, as the real-time times
planned_stops AS (
    SELECT
        trip_id,
        stop_id,
        stop_sequence,
        planned_arrival,
        coalesce(planned_departure, planned_arrival) AS planned_departure,
        stop_sequence = max(stop_sequence) OVER (PARTITION BY trip_id) AS is_terminus
    FROM (
        SELECT
            t.trip_id,
            st.stop_id,
            st.stop_sequence,
            timezone('UTC', timezone('Europe/Paris', {{ run_date() }} + to_seconds(
                split_part(st.arrival_time, ':', 1)::INT * 3600
                + split_part(st.arrival_time, ':', 2)::INT * 60
                + split_part(st.arrival_time, ':', 3)::INT
            ))) AS planned_arrival,
            timezone('UTC', timezone('Europe/Paris', {{ run_date() }} + to_seconds(
                split_part(st.departure_time, ':', 1)::INT * 3600
                + split_part(st.departure_time, ':', 2)::INT * 60
                + split_part(st.departure_time, ':', 3)::INT
            ))) AS planned_departure
        FROM {{ source("silver", "stop_times") }} AS st
        INNER JOIN trains AS t
            ON st.trip_id = t.trip_id
            AND {{ gtfs_version("st.filename") }} = t.last_gtfs_version
        -- Filtering on the file name only reads the files of these versions
        WHERE {{ gtfs_version("st.filename") }}
            BETWEEN {{ run_date() }} - {{ lookback_versions - 1 }} AND {{ run_date() }}
    )
),

feed AS (
    SELECT
        trip_id,
        NULLIF(regexp_extract(trip_id, '^[A-Z]+[0-9]+[A-Z]'), '') AS feed_train_number,
        stop_id,
        stop_position,
        stop_schedule_relationship,
        arrival_delay,
        departure_delay,
        arrival_time,
        departure_time,
        fetched_at
    FROM {{ source("silver", "trip_updates") }}
    WHERE start_date = {{ run_date() }}
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

-- One row per train, snapshot and stop sent by the feed. A train sent under
-- both ids in one snapshot keeps its full id
feed_stops AS (
    SELECT
        m.trip_id,
        u.fetched_at,
        u.stop_id,
        arg_min(
            {
                -- Delay at the stop: arrival, departure at the origin (no arrival)
                'delay_raw': coalesce(u.arrival_delay, u.departure_delay),
                'feed_departure': coalesce(u.departure_time, u.arrival_time),
                'feed_arrival': coalesce(u.arrival_time, u.departure_time),
                'relationship': u.stop_schedule_relationship
            },
            (NOT m.is_full_id)::INT * 1000 + u.stop_position
        ) AS sent
    FROM feed AS u
    INNER JOIN id_map AS m
        ON u.trip_id = m.feed_trip_id
    GROUP BY m.trip_id, u.fetched_at, u.stop_id
),

snapshots AS (
    SELECT DISTINCT fetched_at
    FROM feed
),

-- Snapshots that contain the train, with the next one
observations AS (
    SELECT
        trip_id,
        fetched_at,
        lead(fetched_at) OVER (PARTITION BY trip_id ORDER BY fetched_at) AS next_fetched_at
    FROM (SELECT DISTINCT trip_id, fetched_at FROM feed_stops)
),

-- Running window: from the planned departure at the origin to the latest
-- arrival, planned or sent by the feed (a late train runs past its plan)
windows AS (
    SELECT
        p.trip_id,
        p.window_start,
        greatest(p.planned_end, coalesce(f.feed_end, p.planned_end)) AS window_end,
        f.first_fetched_at
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
            max(sent.feed_arrival) AS feed_end,
            min(fetched_at) AS first_fetched_at
        FROM feed_stops
        GROUP BY trip_id
    ) AS f
        ON p.trip_id = f.trip_id
),

-- Every planned stop at every snapshot that contains the train, with the
-- last delay and status sent for the stop up to that snapshot
observed_stops AS (
    SELECT
        o.trip_id,
        o.fetched_at,
        o.next_fetched_at,
        ps.stop_id,
        ps.stop_sequence,
        ps.is_terminus,
        ps.planned_arrival,
        ps.planned_departure,
        fs.sent.delay_raw,
        fs.sent.feed_departure,
        fs.sent.feed_arrival,
        last_value(fs.sent.delay_raw IGNORE NULLS) OVER stop_history AS last_known_delay,
        last_value(fs.sent.relationship IGNORE NULLS) OVER stop_history AS last_known_relationship
    FROM observations AS o
    INNER JOIN planned_stops AS ps
        ON o.trip_id = ps.trip_id
    LEFT JOIN feed_stops AS fs
        ON o.trip_id = fs.trip_id
        AND o.fetched_at = fs.fetched_at
        AND ps.stop_id = fs.stop_id
    WINDOW stop_history AS (
        PARTITION BY o.trip_id, ps.stop_sequence
        ORDER BY o.fetched_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

-- Delay and passing time of every stop in each state of the train, and the
-- times the state holds for. Snapshot times are exact, so bounds are
-- strict except the observed snapshot itself (state_start = state_end).
states AS (
    -- In the snapshot: what the feed sent
    SELECT
        trip_id,
        stop_id,
        stop_sequence,
        true AS is_observed,
        fetched_at AS state_start,
        fetched_at AS state_end,
        delay_raw,
        delay,
        CASE
            WHEN last_known_relationship = 'SKIPPED' THEN '-infinity'::TIMESTAMP
            WHEN is_terminus THEN coalesce(feed_arrival, planned_arrival + to_seconds(delay))
            ELSE coalesce(feed_departure, planned_departure + to_seconds(delay))
        END AS passing_time
    FROM (
        SELECT
            *,
            coalesce(delay_raw, {{ absent_delay }}) AS delay
        FROM observed_stops
    )

    UNION ALL

    -- After the snapshot, until the next one with the train: absent
    SELECT
        trip_id,
        stop_id,
        stop_sequence,
        false AS is_observed,
        fetched_at AS state_start,
        coalesce(next_fetched_at, 'infinity'::TIMESTAMP) AS state_end,
        NULL AS delay_raw,
        delay,
        CASE
            WHEN last_known_relationship = 'SKIPPED' THEN '-infinity'::TIMESTAMP
            WHEN is_terminus THEN planned_arrival + to_seconds(delay)
            ELSE planned_departure + to_seconds(delay)
        END AS passing_time
    FROM (
        SELECT
            *,
            {{ absent_delay }} AS delay
        FROM observed_stops
    )

    UNION ALL

    -- Before the first snapshot with the train, or never seen: planned times
    SELECT
        ps.trip_id,
        ps.stop_id,
        ps.stop_sequence,
        false AS is_observed,
        '-infinity'::TIMESTAMP AS state_start,
        coalesce(w.first_fetched_at, 'infinity'::TIMESTAMP) AS state_end,
        NULL AS delay_raw,
        0 AS delay,
        CASE WHEN ps.is_terminus THEN ps.planned_arrival ELSE ps.planned_departure END
            AS passing_time
    FROM planned_stops AS ps
    INNER JOIN windows AS w
        ON ps.trip_id = w.trip_id
),

-- A stop is the current one from the time all earlier stops are passed
-- until it is passed itself: [current_from, passing_time)
intervals AS (
    SELECT
        s.*,
        w.window_start,
        w.window_end,
        coalesce(
            max(s.passing_time) OVER (
                PARTITION BY s.trip_id, s.is_observed, s.state_start
                ORDER BY s.stop_sequence
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            '-infinity'::TIMESTAMP
        ) AS current_from
    FROM states AS s
    INNER JOIN windows AS w
        ON s.trip_id = w.trip_id
),

-- Only the intervals that can hold a snapshot of the running window
useful_intervals AS (
    SELECT
        *,
        greatest(current_from, window_start) AS range_from,
        least(passing_time, state_end) AS range_to
    FROM intervals
    WHERE current_from < passing_time
        AND (
            (is_observed AND state_start >= current_from AND state_start < passing_time
                AND state_start BETWEEN window_start AND window_end)
            OR (NOT is_observed AND greatest(current_from, window_start) < least(passing_time, state_end)
                AND least(passing_time, state_end) > state_start)
        )
)

-- Next stop not passed; a train with every stop passed has arrived: no row
SELECT
    {{ run_date() }} AS date,
    trip_id,
    state_start AS fetched_at,
    stop_id,
    stop_sequence,
    is_observed,
    delay_raw::BIGINT AS delay_raw,
    delay::BIGINT AS delay
FROM useful_intervals
WHERE is_observed

UNION ALL

SELECT
    {{ run_date() }} AS date,
    i.trip_id,
    s.fetched_at,
    i.stop_id,
    i.stop_sequence,
    i.is_observed,
    i.delay_raw::BIGINT AS delay_raw,
    i.delay::BIGINT AS delay
FROM useful_intervals AS i
INNER JOIN snapshots AS s
    ON s.fetched_at >= i.range_from
    AND s.fetched_at < i.range_to
WHERE NOT i.is_observed
    AND s.fetched_at > i.state_start
    AND s.fetched_at <= i.window_end
