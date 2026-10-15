{#-
    Without the GTFS version of the run date, every train of the day would
    look cancelled (missing from the D version). Fails when no train is
    "planned", i.e. when that version is missing.
-#}
SELECT count(*) AS planned_trains
FROM {{ ref("int_planned_trains") }}
HAVING count(*) FILTER (WHERE gtfs_status = 'planned') = 0
