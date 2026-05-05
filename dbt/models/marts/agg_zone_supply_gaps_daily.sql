{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_date', 'pu_location_id'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_date', 'pu_location_id'],
    )
}}

-- Q3: longest pickup gap per (date, zone). LAG() partitioned by
-- (pickup_date, pu_location_id) — never crosses month boundaries, so
-- rebuilding just target_month's days is safe.
-- Yearly rollup lives in agg_zone_supply_gaps_yearly.

with trips as (
    select
        pickup_date,
        pu_location_id,
        pu_borough,
        pu_zone,
        pickup_ts
    from {{ ref('int_trips_enriched') }}
    {% if is_incremental() %}
    where pickup_year  = {{ var('target_year') }}
      and pickup_month = {{ var('target_month') }}
    {% endif %}
),

with_gaps as (
    select
        pickup_date,
        pu_location_id,
        pu_borough,
        pu_zone,
        pickup_ts,
        datediff(
            'minute',
            lag(pickup_ts) over (
                partition by pickup_date, pu_location_id
                order by pickup_ts
            ),
            pickup_ts
        ) as gap_min
    from trips
)

select
    pickup_date,
    pu_location_id,
    any_value(pu_borough)               as pu_borough,
    any_value(pu_zone)                  as pu_zone,
    count(*)                            as trip_count,
    max(gap_min)                        as longest_gap_min,
    avg(gap_min)                        as avg_gap_min,
    count_if(gap_min > 60)              as gaps_gt_1h,
    count_if(gap_min > 180)             as gaps_gt_3h,
    min(pickup_ts)                      as first_pickup_ts,
    max(pickup_ts)                      as last_pickup_ts,
    current_timestamp()                 as mart_built_at
from with_gaps
group by 1, 2
