{#-
    Features of the delay prediction: one row per train announced for day D,
    with what is known before D starts. Every input is strictly before D: the
    trains and their planned times come from the latest GTFS version published
    before D, the histories and alerts stop at D-1. The same partition serves
    the training (joined later to the target in fact_train_trips of D) and the
    daily prediction, so both see the same features (see business_rules.md).
    The target is not stored here: the partition of D can be built before D.
    Reads the Gold files of fact_train_trips and fact_train_alerts of the days
    before D (see the gold_history macro): built after both jobs of D-1.
    Written to {external_root}/ml_delay_features/date=YYYY-MM-DD/data_0.parquet;
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
        tags=["ml"]
    )
}}

-- depends_on: {{ ref("fact_train_trips") }}
-- depends_on: {{ ref("fact_train_alerts") }}

{%- set late_seconds = 300 %}
{%- set history_days = 28 %}
{%- set version_lookback = 7 %}

-- Latest GTFS version published before D that announces trains for D
WITH version AS (
    SELECT max({{ gtfs_version() }}) AS gtfs_version
    FROM {{ source("silver", "calendar_dates") }}
    -- Filtering on the file name only reads the files of these versions
    WHERE {{ gtfs_version() }}
        BETWEEN {{ run_date() }} - {{ version_lookback }} AND {{ run_date() }} - 1
        AND date = {{ run_date() }}
        -- 1 = service added on that day (the SNCF feed only publishes additions)
        AND exception_type = 1
),

-- Trains of day D in that version
announced AS (
    SELECT DISTINCT
        t.trip_id,
        NULLIF(regexp_extract(t.trip_id, '^[A-Z]+[0-9]+[A-Z]'), '') AS train_number,
        t.route_id,
        v.gtfs_version
    FROM {{ source("silver", "trips") }} AS t
    INNER JOIN version AS v
        ON {{ gtfs_version("t.filename") }} = v.gtfs_version
    INNER JOIN {{ source("silver", "calendar_dates") }} AS c
        ON {{ gtfs_version("c.filename") }} = v.gtfs_version
        AND c.service_id = t.service_id
    WHERE {{ gtfs_version("t.filename") }}
        BETWEEN {{ run_date() }} - {{ version_lookback }} AND {{ run_date() }} - 1
        AND {{ gtfs_version("c.filename") }}
            BETWEEN {{ run_date() }} - {{ version_lookback }} AND {{ run_date() }} - 1
        AND c.date = {{ run_date() }}
        AND c.exception_type = 1
),

-- GTFS times are HH:MM:SS local times from midnight of the service day and
-- can go past 24:00: converted to seconds from midnight
planned AS (
    SELECT
        st.trip_id,
        min(
            split_part(st.departure_time, ':', 1)::INT * 3600
            + split_part(st.departure_time, ':', 2)::INT * 60
            + split_part(st.departure_time, ':', 3)::INT
        ) AS first_departure_seconds,
        max(
            split_part(st.arrival_time, ':', 1)::INT * 3600
            + split_part(st.arrival_time, ':', 2)::INT * 60
            + split_part(st.arrival_time, ':', 3)::INT
        ) AS last_arrival_seconds,
        count(*) AS planned_stops
    FROM {{ source("silver", "stop_times") }} AS st
    INNER JOIN announced AS a
        ON st.trip_id = a.trip_id
        AND {{ gtfs_version("st.filename") }} = a.gtfs_version
    WHERE {{ gtfs_version("st.filename") }}
        BETWEEN {{ run_date() }} - {{ version_lookback }} AND {{ run_date() }} - 1
    GROUP BY st.trip_id
),

-- Announced trains of the days before D. The delay is only kept when it was
-- measured (train seen running, not cancelled), early arrivals count as 0
history AS (
    SELECT
        date,
        train_number,
        route_id,
        is_cancelled,
        CASE
            WHEN is_observed AND NOT is_cancelled
                THEN greatest(terminus_arrival_delay, 0)
        END AS delay
    FROM {{ gold_history("fact_train_trips") }}
    WHERE date BETWEEN {{ run_date() }} - {{ history_days }} AND {{ run_date() }} - 1
        AND train_status <> 'added'
),

-- avg() skips nulls: rates and means are over the days with a measured delay
train_history AS (
    SELECT
        train_number,
        count(delay) AS train_runs_28d,
        avg((delay >= {{ late_seconds }})::INT) AS train_late_rate_28d,
        avg((delay >= {{ late_seconds }})::INT)
            FILTER (WHERE date >= {{ run_date() }} - 7) AS train_late_rate_7d,
        avg(delay) / 60 AS train_mean_delay_28d,
        avg(is_cancelled::INT) AS train_cancel_rate_28d,
        arg_max(delay, date) FILTER (WHERE delay IS NOT NULL) / 60 AS train_last_delay,
        {{ run_date() }} - max(date) FILTER (WHERE delay IS NOT NULL) AS train_days_since_last_run
    FROM history
    WHERE train_number IS NOT NULL
    GROUP BY train_number
),

route_history AS (
    SELECT
        route_id,
        avg((delay >= {{ late_seconds }})::INT) AS route_late_rate_28d,
        avg((delay >= {{ late_seconds }})::INT)
            FILTER (WHERE date >= {{ run_date() }} - 7) AS route_late_rate_7d,
        avg((delay >= {{ late_seconds }})::INT)
            FILTER (WHERE date = {{ run_date() }} - 1) AS route_late_rate_1d,
        avg(delay) FILTER (WHERE date >= {{ run_date() }} - 7) / 60 AS route_mean_delay_7d
    FROM history
    GROUP BY route_id
),

network_history AS (
    SELECT avg((delay >= {{ late_seconds }})::INT) AS network_late_rate_1d
    FROM history
    WHERE date = {{ run_date() }} - 1
),

-- Alerts of D-1. Null when there is no alert file for D-1 (alerts are
-- collected since 2026-10-07), so that missing data is not read as 0 alerts
alerts AS (
    SELECT alert_id, train_number, route_id
    FROM {{ gold_history("fact_train_alerts") }}
    WHERE date = {{ run_date() }} - 1
),

alerts_available AS (
    SELECT count(*) > 0 AS is_available
    FROM alerts
),

train_alerts AS (
    SELECT train_number, count(DISTINCT alert_id) AS alerts
    FROM alerts
    GROUP BY train_number
),

route_alerts AS (
    SELECT route_id, count(DISTINCT alert_id) AS alerts
    FROM alerts
    WHERE route_id IS NOT NULL
    GROUP BY route_id
)

SELECT
    {{ run_date() }} AS date,
    a.trip_id,
    a.train_number,
    a.route_id,
    a.gtfs_version,
    isodow({{ run_date() }})::INT AS day_of_week,
    (p.first_departure_seconds // 3600 % 24)::INT AS departure_hour,
    ((p.last_arrival_seconds - p.first_departure_seconds) // 60)::INT AS planned_duration_minutes,
    p.planned_stops::INT AS planned_stops,
    coalesce(th.train_runs_28d, 0)::INT AS train_runs_28d,
    th.train_late_rate_28d::DOUBLE AS train_late_rate_28d,
    th.train_late_rate_7d::DOUBLE AS train_late_rate_7d,
    th.train_mean_delay_28d::DOUBLE AS train_mean_delay_28d,
    th.train_cancel_rate_28d::DOUBLE AS train_cancel_rate_28d,
    th.train_last_delay::DOUBLE AS train_last_delay,
    th.train_days_since_last_run::INT AS train_days_since_last_run,
    rh.route_late_rate_28d::DOUBLE AS route_late_rate_28d,
    rh.route_late_rate_7d::DOUBLE AS route_late_rate_7d,
    rh.route_late_rate_1d::DOUBLE AS route_late_rate_1d,
    rh.route_mean_delay_7d::DOUBLE AS route_mean_delay_7d,
    nh.network_late_rate_1d::DOUBLE AS network_late_rate_1d,
    CASE WHEN aa.is_available THEN coalesce(ta.alerts, 0) END::INT AS train_alerts_1d,
    CASE WHEN aa.is_available THEN coalesce(ra.alerts, 0) END::INT AS route_alerts_1d
FROM announced AS a
LEFT JOIN planned AS p
    ON a.trip_id = p.trip_id
LEFT JOIN train_history AS th
    ON a.train_number = th.train_number
LEFT JOIN route_history AS rh
    ON a.route_id = rh.route_id
CROSS JOIN network_history AS nh
CROSS JOIN alerts_available AS aa
LEFT JOIN train_alerts AS ta
    ON a.train_number = ta.train_number
LEFT JOIN route_alerts AS ra
    ON a.route_id = ra.route_id
