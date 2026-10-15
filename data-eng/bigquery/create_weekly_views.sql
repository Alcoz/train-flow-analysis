-- Weekly views of the delay and cancellation indicators, on top of
-- gold.agg_daily_route (see data-eng/docs/business_rules.md).
-- A week is the sum of its days: agg_daily_route only holds counts and sums.
-- Rates are computed in the dashboards, as ratios of these sums.
--
-- Run in the BigQuery console after create_gold_external_tables.sql.
-- Safe to re-run: every statement is CREATE OR REPLACE.

SET @@location = 'EU';


-- Whole network per ISO week (Monday to Sunday), with what is needed to spot
-- and remove incomplete weeks:
--   days_in_week: days present in agg_daily_route (7 for a complete week)
--   min_daily_observed_share: lowest share of announced trains seen in the
--     real-time feed on a day of the week; a collection gap makes it drop
--     even when the day is present.
CREATE OR REPLACE VIEW gold.weekly_network AS
WITH daily AS (
  SELECT
    date,
    SUM(announced_trains) AS announced_trains,
    SUM(planned_trains) AS planned_trains,
    SUM(late_cancelled_trains) AS late_cancelled_trains,
    SUM(deprogrammed_trains) AS deprogrammed_trains,
    SUM(added_trains) AS added_trains,
    SUM(observed_trains) AS observed_trains,
    SUM(limited_trains) AS limited_trains,
    SUM(delay_measured_trains) AS delay_measured_trains,
    SUM(terminus_delay_seconds) AS terminus_delay_seconds,
    -- Added trains are observed but not announced: kept out of the share
    SAFE_DIVIDE(SUM(observed_trains) - SUM(added_trains), SUM(announced_trains))
      AS observed_share
  FROM gold.agg_daily_route
  GROUP BY date
)
SELECT
  DATE_TRUNC(date, ISOWEEK) AS week_start,
  COUNT(*) AS days_in_week,
  COUNT(*) = 7 AS is_complete_week,
  MIN(observed_share) AS min_daily_observed_share,
  SUM(announced_trains) AS announced_trains,
  SUM(planned_trains) AS planned_trains,
  SUM(late_cancelled_trains) AS late_cancelled_trains,
  SUM(deprogrammed_trains) AS deprogrammed_trains,
  SUM(added_trains) AS added_trains,
  SUM(observed_trains) AS observed_trains,
  SUM(limited_trains) AS limited_trains,
  SUM(delay_measured_trains) AS delay_measured_trains,
  SUM(terminus_delay_seconds) AS terminus_delay_seconds
FROM daily
GROUP BY week_start;


-- Per line and ISO week, with the rankings of the "worst lines" visuals:
--   delay_rank / late_cancelled_rank: 1 = worst line of the week
--   previous_*: value of the line the week before, for the week-on-week gap
--   weeks_in_lead_*: consecutive weeks the line has been first, up to this
--     week (null when it is not first)
-- Trains added in real time have no line (null route_id): they are kept in
-- the view but not ranked.
CREATE OR REPLACE VIEW gold.weekly_route AS
WITH weekly AS (
  SELECT
    DATE_TRUNC(a.date, ISOWEEK) AS week_start,
    a.route_id,
    SUM(a.announced_trains) AS announced_trains,
    SUM(a.planned_trains) AS planned_trains,
    SUM(a.late_cancelled_trains) AS late_cancelled_trains,
    SUM(a.deprogrammed_trains) AS deprogrammed_trains,
    SUM(a.added_trains) AS added_trains,
    SUM(a.observed_trains) AS observed_trains,
    SUM(a.limited_trains) AS limited_trains,
    SUM(a.delay_measured_trains) AS delay_measured_trains,
    SUM(a.terminus_delay_seconds) AS terminus_delay_seconds
  FROM gold.agg_daily_route AS a
  GROUP BY week_start, a.route_id
),

ranked AS (
  SELECT
    *,
    IF(route_id IS NULL, NULL,
      RANK() OVER (PARTITION BY week_start, route_id IS NULL ORDER BY terminus_delay_seconds DESC))
      AS delay_rank,
    IF(route_id IS NULL, NULL,
      RANK() OVER (PARTITION BY week_start, route_id IS NULL ORDER BY late_cancelled_trains DESC))
      AS late_cancelled_rank,
    LAG(terminus_delay_seconds) OVER (PARTITION BY route_id ORDER BY week_start)
      AS previous_terminus_delay_seconds,
    LAG(late_cancelled_trains) OVER (PARTITION BY route_id ORDER BY week_start)
      AS previous_late_cancelled_trains,
    LAG(week_start) OVER (PARTITION BY route_id ORDER BY week_start) AS previous_week_start
  FROM weekly
),

-- Streaks of consecutive weeks in first place (gaps and islands): within a
-- streak, week number minus row number is constant.
delay_leaders AS (
  SELECT
    week_start,
    route_id,
    ROW_NUMBER() OVER (PARTITION BY route_id, streak ORDER BY week_start) AS weeks_in_lead
  FROM (
    SELECT
      week_start,
      route_id,
      DATE_DIFF(week_start, DATE '2000-01-03', WEEK(MONDAY))
        - ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY week_start) AS streak
    FROM ranked
    WHERE delay_rank = 1
  )
),

late_cancelled_leaders AS (
  SELECT
    week_start,
    route_id,
    ROW_NUMBER() OVER (PARTITION BY route_id, streak ORDER BY week_start) AS weeks_in_lead
  FROM (
    SELECT
      week_start,
      route_id,
      DATE_DIFF(week_start, DATE '2000-01-03', WEEK(MONDAY))
        - ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY week_start) AS streak
    FROM ranked
    WHERE late_cancelled_rank = 1
  )
),

-- Line names from the last snapshot of each line
routes AS (
  SELECT route_id, route_short_name, route_long_name
  FROM gold.dim_routes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY date DESC) = 1
)

SELECT
  r.week_start,
  r.route_id,
  n.route_short_name,
  n.route_long_name,
  w.is_complete_week,
  w.min_daily_observed_share,
  r.announced_trains,
  r.planned_trains,
  r.late_cancelled_trains,
  r.deprogrammed_trains,
  r.added_trains,
  r.observed_trains,
  r.limited_trains,
  r.delay_measured_trains,
  r.terminus_delay_seconds,
  r.delay_rank,
  r.late_cancelled_rank,
  -- Only when the line also has data the week just before
  IF(r.previous_week_start = DATE_SUB(r.week_start, INTERVAL 1 WEEK),
    r.previous_terminus_delay_seconds, NULL) AS previous_terminus_delay_seconds,
  IF(r.previous_week_start = DATE_SUB(r.week_start, INTERVAL 1 WEEK),
    r.previous_late_cancelled_trains, NULL) AS previous_late_cancelled_trains,
  d.weeks_in_lead AS weeks_in_lead_delay,
  c.weeks_in_lead AS weeks_in_lead_late_cancelled
FROM ranked AS r
LEFT JOIN gold.weekly_network AS w
  ON r.week_start = w.week_start
LEFT JOIN routes AS n
  ON r.route_id = n.route_id
LEFT JOIN delay_leaders AS d
  ON r.week_start = d.week_start AND r.route_id = d.route_id
LEFT JOIN late_cancelled_leaders AS c
  ON r.week_start = c.week_start AND r.route_id = c.route_id;
