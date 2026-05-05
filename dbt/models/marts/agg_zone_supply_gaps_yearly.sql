{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pu_location_id'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year'],
    )
}}

-- Yearly rollup of agg_zone_supply_gaps_daily. Grain: (year, zone).
-- Surfaces zones that *regularly* go extended periods without pickups —
-- Q3's "regularly" interpretation, which the daily mart alone doesn't
-- answer cleanly.

select
    extract(year from pickup_date)::int     as pickup_year,
    pu_location_id,
    any_value(pu_borough)                   as pu_borough,
    any_value(pu_zone)                      as pu_zone,
    sum(trip_count)                         as total_trips,
    count(distinct pickup_date)             as days_with_trips,
    max(longest_gap_min)                    as longest_gap_min_year,
    avg(longest_gap_min)                    as avg_longest_gap_per_day,
    count_if(gaps_gt_1h > 0)                as days_with_gap_over_60min,
    count_if(gaps_gt_3h > 0)                as days_with_gap_over_3h,
    current_timestamp()                     as mart_built_at
from {{ ref('agg_zone_supply_gaps_daily') }}
{% if is_incremental() %}
where extract(year from pickup_date) = {{ var('target_year') }}
{% endif %}
group by 1, 2
