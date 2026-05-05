{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pickup_month', 'pu_location_id'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pickup_month'],
    )
}}

-- Q1: zone revenue + monthly rank. Grain: (year, month, zone).
-- Yearly rollup (yearly_gross_revenue, yearly_revenue_rank) lives in
-- agg_zone_revenue_yearly, derived from this mart.

select
    pickup_year,
    pickup_month,
    pu_location_id,
    any_value(pu_borough)   as pu_borough,
    any_value(pu_zone)      as pu_zone,
    count(*)                as trip_count,
    sum(total_amount)       as gross_revenue,
    sum(fare_amount)        as fare_revenue,
    sum(tip_amount)         as tip_revenue,
    avg(total_amount)       as avg_revenue_per_trip,
    rank() over (
        partition by pickup_year, pickup_month
        order by sum(total_amount) desc
    )                       as revenue_rank_in_month,
    current_timestamp()     as mart_built_at
from {{ ref('int_trips_enriched') }}
{% if is_incremental() %}
where pickup_year  = {{ var('target_year') }}
  and pickup_month = {{ var('target_month') }}
{% endif %}
group by 1, 2, 3
