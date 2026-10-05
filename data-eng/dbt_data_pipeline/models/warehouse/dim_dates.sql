{{ config(tags=["daily"]) }}

{#-
    The range ends a week after the actual run date (not the partition date),
    so backfilling an old partition never drops dates used by newer facts.
    The end date is exclusive.
-#}
{%- set start_date = "2026-09-01" -%}
{%- set end_date = (run_started_at.date() + modules.datetime.timedelta(days=7)).isoformat() -%}

with dim_dates as (

    {{ dbt_date.get_date_dimension(start_date, end_date) }}

)
SELECT
    date_day,
    day_of_week_iso,
    day_of_week_name,
    day_of_week_name_short,
    day_of_month,
    day_of_year,
    iso_week_start_date,
    iso_week_of_year,
    month_of_year,
    month_name,
    month_name_short,
    year_number
FROM dim_dates
