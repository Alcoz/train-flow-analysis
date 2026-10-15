{#-
    One row per service alert present in the feed on the run date, with its
    category and reason from the alert_classification_rules seed.
    The SNCF edits alerts over time (it removes the numbers of trains that
    have passed), so the version kept is the one of the last snapshot of the
    day: it is what the alert said about the trains of that day.
    Kept in the in-memory DuckDB of the run, not written to Gold.
-#}
{{
    config(
        materialized='table',
        tags=["daily"]
    )
}}

WITH last_snapshot AS (
    SELECT *
    FROM {{ source("silver", "alerts") }}
    QUALIFY row_number() OVER (PARTITION BY alert_id ORDER BY fetched_at DESC) = 1
),

classified AS (
    SELECT
        a.alert_id,
        r.rule_id,
        r.category,
        r.reason
    FROM last_snapshot AS a
    -- Rules are written lowercase and without accents
    LEFT JOIN {{ ref("alert_classification_rules") }} AS r
        ON regexp_matches(
            lower(strip_accents(
                coalesce(a.header_text, '') || ' | ' || coalesce(a.description_text, '')
            )),
            r.pattern
        )
        AND (r.cause_filter IS NULL OR r.cause_filter = coalesce(a.cause, 'absent'))
    -- Lowest priority wins
    QUALIFY row_number() OVER (
        PARTITION BY a.alert_id ORDER BY r.priority NULLS LAST, r.rule_id
    ) = 1
)

SELECT
    {{ run_date() }} AS date,
    a.alert_id,
    a.cause,
    a.header_text,
    a.description_text,
    a.active_periods,
    a.informed_entities,
    a.fetched_at,
    c.rule_id,
    -- No rule matched: a new SNCF message format, to add to the rules
    coalesce(c.category, 'autre') AS category,
    c.reason
FROM last_snapshot AS a
INNER JOIN classified AS c
    ON a.alert_id = c.alert_id
