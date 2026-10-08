WITH trip_delays AS (
    SELECT
        date,
        trip_id,
        route_id,
        MAX(last_arrival_delay) AS trip_delay_s
    FROM gold.fact_train_trips
    WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    GROUP BY date, trip_id, route_id
)
SELECT
    r.route_short_name,
    r.route_long_name,
    COUNT(*)                                                    AS nb_trains,
    ROUND(AVG(t.trip_delay_s) / 60, 1)                          AS retard_moyen_min,
    ROUND(STDDEV_SAMP(t.trip_delay_s) / 60, 1)                  AS std_moyen,
    ROUND(APPROX_QUANTILES(t.trip_delay_s, 100)[OFFSET(25)] / 60, 1) AS retard_p25_min,
    ROUND(APPROX_QUANTILES(t.trip_delay_s, 100)[OFFSET(50)] / 60, 1) AS retard_p50_min,
    ROUND(APPROX_QUANTILES(t.trip_delay_s, 100)[OFFSET(90)] / 60, 1) AS retard_p90_min,
    ROUND(100 * COUNTIF(t.trip_delay_s >= 300) / COUNT(*), 1)   AS pct_trains_retard_5min
FROM trip_delays t
JOIN gold.dim_routes r
    ON r.route_id = t.route_id AND r.date = t.date
GROUP BY r.route_short_name, r.route_long_name
HAVING nb_trains >= 30            -- évite les lignes avec trop peu de trains
ORDER BY retard_moyen_min DESC