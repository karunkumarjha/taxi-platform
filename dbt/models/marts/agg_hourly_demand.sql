{{
    config(
        materialized = 'table',
        cluster_by   = ['pickup_year', 'pickup_month'],
    )
}}

-- Business Q2: How do trip volume and average fare vary by hour-of-day, and
-- when are the peak / trough periods?
--
-- Grain: (pickup_year, pickup_month, pickup_dow, pickup_hour). Monthly grain
-- lets the dashboard roll up to day-of-week × hour heatmap OR drill to a single
-- month. Keeping month here (vs a flat hour-of-day summary) costs us ~24*7*12 =
-- 2016 rows — trivial, and makes the data useful for "did peak hour shift in
-- December?" questions without a new mart.

select
    pickup_year,
    pickup_month,
    pickup_dow,
    pickup_hour,
    count(*)                               as trip_count,
    sum(total_amount)                      as gross_revenue,
    avg(fare_amount)                       as avg_fare,
    avg(total_amount)                      as avg_total,
    avg(trip_distance)                     as avg_distance,
    avg(trip_duration_min)                 as avg_duration_min,
    approx_percentile(fare_amount, 0.5)    as p50_fare,
    approx_percentile(fare_amount, 0.9)    as p90_fare
from {{ ref('int_trips_enriched') }}
group by 1, 2, 3, 4
