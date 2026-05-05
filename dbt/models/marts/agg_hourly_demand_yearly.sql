{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pickup_dow', 'pickup_hour'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year'],
    )
}}

-- Yearly rollup of agg_hourly_demand_monthly. Grain: (year, dow, hour).
-- Trip-weighted averages so the yearly value reflects actual ride
-- distributions, not an average-of-averages.

select
    pickup_year,
    pickup_dow,
    pickup_hour,
    sum(trip_count)                                                     as trip_count,
    sum(gross_revenue)                                                  as gross_revenue,
    sum(avg_fare * trip_count)         / nullif(sum(trip_count), 0)     as avg_fare,
    sum(avg_total * trip_count)        / nullif(sum(trip_count), 0)     as avg_total,
    sum(avg_distance * trip_count)     / nullif(sum(trip_count), 0)     as avg_distance,
    sum(avg_duration_min * trip_count) / nullif(sum(trip_count), 0)     as avg_duration_min,
    current_timestamp()                                                 as mart_built_at
from {{ ref('agg_hourly_demand_monthly') }}
{% if is_incremental() %}
where pickup_year = {{ var('target_year') }}
{% endif %}
group by 1, 2, 3
