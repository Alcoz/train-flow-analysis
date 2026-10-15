{#-
    One row per train announced for the run date D: every train of day D
    found in at least one of the last 30 GTFS versions. It is the
    denominator of the cancellation indicators: the real-time feed only
    mentions trains with something to report, and the GTFS removes trains
    before their day. Alerts only give a train number, so they are linked
    to trains through (train_number, date).
    Kept in the in-memory DuckDB of the run, not written to Gold.
-#}
{{
    config(
        materialized='table',
        tags=["daily"]
    )
}}

{%- set lookback_versions = 30 %}

WITH trips AS (
    SELECT
        {{ gtfs_version() }} AS gtfs_version,
        trip_id,
        -- "OCESN862402F1187_F:TER:..." -> "OCESN862402F", the id alerts refer to
        -- (null when the format is unknown, regexp_extract returns an empty string)
        NULLIF(regexp_extract(trip_id, '^[A-Z]+[0-9]+[A-Z]'), '') AS train_number,
        -- trip_id changes between versions: a train is identified by its number
        coalesce(train_number, trip_id) AS train_key,
        route_id,
        service_id
    FROM {{ source("silver", "trips") }}
    -- Filtering on the file name only reads the files of these versions
    WHERE {{ gtfs_version() }}
        BETWEEN {{ run_date() }} - {{ lookback_versions - 1 }} AND {{ run_date() }}
),

calendar AS (
    SELECT
        {{ gtfs_version() }} AS gtfs_version,
        service_id
    FROM {{ source("silver", "calendar_dates") }}
    WHERE {{ gtfs_version() }}
        BETWEEN {{ run_date() }} - {{ lookback_versions - 1 }} AND {{ run_date() }}
        AND date = {{ run_date() }}
        -- 1 = service added on that day (the SNCF feed only publishes additions)
        AND exception_type = 1
),

-- One row per GTFS version announcing the train for day D. service_id values
-- change from one version to the next: both tables are joined per version.
announcements AS (
    SELECT t.*
    FROM trips AS t
    INNER JOIN calendar AS c
        ON t.gtfs_version = c.gtfs_version
        AND t.service_id = c.service_id
)

SELECT
    {{ run_date() }} AS date,
    any_value(train_number) AS train_number,
    arg_max(trip_id, gtfs_version) AS trip_id,
    arg_max(route_id, gtfs_version) AS route_id,
    max(gtfs_version) AS last_gtfs_version,
    -- Status from the last version announcing the train (see business_rules.md)
    CASE
        WHEN max(gtfs_version) = {{ run_date() }} THEN 'planned'
        -- Missing from the D version, removed less than 3 days before D
        WHEN {{ run_date() }} - max(gtfs_version) <= 3 THEN 'late_cancelled'
        ELSE 'deprogrammed'
    END AS gtfs_status
FROM announcements
GROUP BY train_key
