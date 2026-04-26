{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_date', 'pu_location_id'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_date', 'pu_location_id'],
    )
}}

-- Business Q3: Are there zones that regularly go extended periods with no
-- pickups? What's the longest observed gap per zone per day?
--
-- Grain: one row per (pickup_date, pu_location_id).
-- Metrics:
--   • longest_gap_min   — biggest gap between consecutive pickups within the day
--   • avg_gap_min       — avg gap (gives a fuller picture than just the max)
--   • trip_count        — trips that day (context for the gaps)
--   • gaps_gt_1h / gaps_gt_3h — how many "long" gaps that day
--
-- Month-grain self-healing incremental: detect months where this mart's
-- summed trip_count (rolled up from per-day rows) diverges from FCT_TRIPS'
-- row count for that month, OR a new month has crossed the phantom threshold.
-- The per-zone LAG window then runs over only the divergent months — much
-- cheaper than the original full-history scan.

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
        select
            extract(year  from pickup_date)::int as pickup_year,
            extract(month from pickup_date)::int as pickup_month,
            sum(trip_count) as agg_cnt
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
),

trips as (
    select
        pickup_date,
        pu_location_id,
        pu_borough,
        pu_zone,
        pickup_ts
    from {{ ref('int_trips_enriched') }}
    where (pickup_year, pickup_month) in (
        select pickup_year, pickup_month from months_to_build
    )
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
    any_value(pu_borough)                                         as pu_borough,
    any_value(pu_zone)                                            as pu_zone,
    count(*)                                                      as trip_count,
    max(gap_min)                                                  as longest_gap_min,
    avg(gap_min)                                                  as avg_gap_min,
    count_if(gap_min > 60)                                        as gaps_gt_1h,
    count_if(gap_min > 180)                                       as gaps_gt_3h,
    min(pickup_ts)                                                as first_pickup_ts,
    max(pickup_ts)                                                as last_pickup_ts
from with_gaps
group by 1, 2
