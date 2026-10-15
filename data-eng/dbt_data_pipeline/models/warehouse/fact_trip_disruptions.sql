{#-
    One row per alert and affected train of the service date. A train of day
    D is affected by an alert when, in the last snapshot of day D, the alert
    lists its train number and D falls within an active period of the alert
    (see business_rules.md). Alerts only give train numbers, with no date:
    fact_trip_runs turns them into trains.
    Written to {external_root}/fact_trip_disruptions/date=YYYY-MM-DD/data_0.parquet;
    overwrite_or_ignore only replaces the partition of this run, so the run
    must only emit rows for its own date.
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

WITH alert_trains AS (
    SELECT DISTINCT
        a.alert_id,
        a.category,
        a.reason,
        a.rule_id,
        e.entity.trip_id AS train_number
    FROM {{ ref("int_alerts") }} AS a,
        unnest(a.informed_entities) AS e(entity)
    -- Entities with only a stop or a line are not linked to trains
    WHERE e.entity.trip_id IS NOT NULL
        -- An active period overlaps day D. Periods are in UTC and D is a
        -- day in Paris: periods are converted first. A null bound is open.
        AND EXISTS (
            SELECT 1
            FROM unnest(a.active_periods) AS p(period)
            WHERE coalesce(
                    (p.period.start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris'
                    < {{ run_date() }} + INTERVAL 1 DAY,
                    true
                )
                AND coalesce(
                    (p.period."end" AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris'
                    >= {{ run_date() }},
                    true
                )
        )
)

SELECT
    r.date,
    a.alert_id,
    r.trip_id,
    r.train_number,
    -- Null for trains added in real time
    r.route_id,
    r.train_status,
    a.category,
    a.reason,
    a.rule_id
FROM alert_trains AS a
INNER JOIN {{ ref("fact_trip_runs") }} AS r
    ON a.train_number = r.train_number
WHERE r.date = {{ run_date() }}
