{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pickup_month', 'pickup_dow', 'pickup_hour'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pickup_month'],
    )
}}

-- Q2: hour × DOW demand + avg fare. Grain: (year, month, dow, hour).
-- Yearly rollup lives in agg_hourly_demand_yearly.

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
    approx_percentile(fare_amount, 0.9)    as p90_fare,
    current_timestamp()                    as mart_built_at
from {{ ref('int_trips_enriched') }}
{% if is_incremental() %}
where pickup_year  = {{ var('target_year') }}
  and pickup_month = {{ var('target_month') }}
{% endif %}
group by 1, 2, 3, 4
