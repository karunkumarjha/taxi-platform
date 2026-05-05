{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pu_location_id'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year'],
    )
}}

-- Yearly rollup of agg_zone_revenue_monthly. Grain: (year, zone).
-- Materialised as a table — reads from the monthly mart (~3k rows/year),
-- not FCT_TRIPS, so the whole-year rebuild on each run is trivial.

select
    pickup_year,
    pu_location_id,
    any_value(pu_borough)                                                       as pu_borough,
    any_value(pu_zone)                                                          as pu_zone,
    sum(trip_count)                                                             as trip_count,
    sum(gross_revenue)                                                          as yearly_gross_revenue,
    sum(fare_revenue)                                                           as yearly_fare_revenue,
    sum(tip_revenue)                                                            as yearly_tip_revenue,
    sum(gross_revenue) / nullif(sum(trip_count), 0)                             as avg_revenue_per_trip,
    rank() over (partition by pickup_year order by sum(gross_revenue) desc)     as yearly_revenue_rank,
    current_timestamp()                                                         as mart_built_at
from {{ ref('agg_zone_revenue_monthly') }}
{% if is_incremental() %}
where pickup_year = {{ var('target_year') }}
{% endif %}
group by 1, 2
