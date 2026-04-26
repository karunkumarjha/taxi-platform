{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pickup_month', 'pickup_dow', 'pickup_hour'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pickup_month'],
    )
}}

-- Business Q2: How do trip volume and average fare vary by hour-of-day, and
-- when are the peak / trough periods?
--
-- Grain: (pickup_year, pickup_month, pickup_dow, pickup_hour).
--
-- Self-healing month-grain incremental: detect months where this mart's
-- summed trip_count diverges from FCT_TRIPS' row count for that month. Two
-- divergence cases are handled:
--   1. Existing month with new rows (TLC tail-bleed merge): rebuild → trip
--      counts and aggregations refresh to include the new rows.
--   2. New month with enough rows to be "real" (>= phantom_month_threshold):
--      first-time build for that month. Phantom partial-months from
--      cross-file leaks (typically <1k rows) are skipped until the real
--      month's data arrives and crosses the threshold.

with fct_month_counts as (
    select pickup_year, pickup_month, count(*) as fct_cnt
    from {{ ref('int_trips_enriched') }}
    group by 1, 2
),

months_to_build as (
    select fct.pickup_year, fct.pickup_month
    from fct_month_counts fct
    {% if is_incremental() %}
    left join (
        select pickup_year, pickup_month, sum(trip_count) as agg_cnt
        from {{ this }}
        group by 1, 2
    ) agg using (pickup_year, pickup_month)
    where
        (agg.agg_cnt is not null and agg.agg_cnt != fct.fct_cnt)
        or
        (agg.agg_cnt is null and fct.fct_cnt >= {{ var('phantom_month_threshold') }})
    {% else %}
    where fct.fct_cnt >= {{ var('phantom_month_threshold') }}
    {% endif %}
)

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
where (pickup_year, pickup_month) in (
    select pickup_year, pickup_month from months_to_build
)
group by 1, 2, 3, 4
