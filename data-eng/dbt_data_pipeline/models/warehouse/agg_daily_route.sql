{#-
    Daily counts per line, from fact_trip_runs. Only counts and sums are
    stored: weeks are sums of days, and rates are computed when reading
    (ratio of sums), so that days with few trains do not weigh as much as busy
    ones. Trains added in real time have no line: they are grouped under a
    null route_id.
    Written to {external_root}/agg_daily_route/date=YYYY-MM-DD/data_0.parquet;
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

SELECT
    date,
    route_id,
    -- Denominator of the cancellation indicators: trains in the GTFS
    count(*) FILTER (WHERE train_status <> 'added') AS announced_trains,
    count(*) FILTER (WHERE train_status = 'planned') AS planned_trains,
    count(*) FILTER (WHERE train_status = 'late_cancelled') AS late_cancelled_trains,
    count(*) FILTER (WHERE train_status = 'deprogrammed') AS deprogrammed_trains,
    count(*) FILTER (WHERE train_status = 'added') AS added_trains,
    count(*) FILTER (WHERE is_observed) AS observed_trains,
    count(*) FILTER (WHERE is_limited) AS limited_trains,
    -- Delay at the effective terminus of the trains that ran; early arrivals
    -- count as 0 so that they do not offset the delay of other trains
    count(terminus_arrival_delay) FILTER (WHERE NOT is_cancelled) AS delay_measured_trains,
    -- sum() returns a HUGEINT, which BigQuery cannot read from Parquet
    coalesce(
        sum(greatest(terminus_arrival_delay, 0)) FILTER (WHERE NOT is_cancelled), 0
    )::BIGINT AS terminus_delay_seconds
FROM {{ ref("fact_trip_runs") }}
WHERE date = {{ run_date() }}
GROUP BY date, route_id
