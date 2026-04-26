{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pu_location_id', 'distance_bucket', 'payment_type'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pu_location_id'],
    )
}}

-- Business Q4: What's the relationship between trip distance, payment type,
-- and tip percentage? Are there zones where passengers tip significantly
-- more or less?
--
-- Grain: (pickup_year, pu_location_id, distance_bucket, payment_type).
-- Year-grain (no month in grain). Each per-run rebuild replaces a year's
-- complete slice from FCT_TRIPS.
--
-- Year-grain self-healing incremental: same detection pattern as
-- agg_zone_revenue_monthly — rebuild any year whose total row count diverges
-- from FCT_TRIPS for that year.
--
-- Cost tradeoff (deliberate): same as agg_zone_revenue_monthly — a divergent
-- year triggers a full O(year-rows) rebuild even though only a fraction
-- changed. Accepted because the grain itself is year-level (no month in
-- unique_key), so partial rebuilds aren't naturally expressible. Cheap at
-- 38M-row scale; revisit with streams + tasks at 1.5B-row scale.
--
-- Key caveat, documented in the README: tip_amount is reliable only for
-- payment_type = 1 (credit). Cash tips (payment_type = 2) are NOT recorded by
-- the meter. A zone's "low tip" signal can be an artifact of cash-heavy
-- ridership rather than tipping behaviour. We keep all payment types in the
-- mart so the dashboard can expose this explicitly (filter to credit-only when
-- analysing actual tip %).

with fct_year_counts as (
    select pickup_year, count(*) as fct_cnt
    from {{ ref('int_trips_enriched') }}
    group by 1
),

years_to_rebuild as (
    select fct.pickup_year
    from fct_year_counts fct
    {% if is_incremental() %}
    left join (
        select pickup_year, sum(trip_count) as agg_cnt
        from {{ this }}
        group by 1
    ) agg using (pickup_year)
    where
        (agg.agg_cnt is not null and agg.agg_cnt != fct.fct_cnt)
        or
        (agg.agg_cnt is null and fct.fct_cnt >= {{ var('phantom_month_threshold') }})
    {% else %}
    where fct.fct_cnt >= {{ var('phantom_month_threshold') }}
    {% endif %}
),

enriched as (
    select
        pickup_year,
        pu_location_id,
        pu_borough,
        pu_zone,
        payment_type,
        case
            when trip_distance <= 1   then '0-1mi'
            when trip_distance <= 3   then '1-3mi'
            when trip_distance <= 5   then '3-5mi'
            when trip_distance <= 10  then '5-10mi'
            when trip_distance <= 20  then '10-20mi'
            else                           '20mi+'
        end                                 as distance_bucket,
        fare_amount,
        tip_amount,
        total_amount,
        tip_pct
    from {{ ref('int_trips_enriched') }}
    where pickup_year in (select pickup_year from years_to_rebuild)
)

select
    pickup_year,
    pu_location_id,
    any_value(pu_borough)                           as pu_borough,
    any_value(pu_zone)                              as pu_zone,
    distance_bucket,
    payment_type,
    count(*)                                        as trip_count,
    avg(fare_amount)                                as avg_fare,
    avg(tip_amount)                                 as avg_tip,
    avg(case when payment_type = 1 then tip_pct end)                      as avg_tip_pct_credit,
    approx_percentile(case when payment_type = 1 then tip_pct end, 0.5)   as p50_tip_pct_credit,
    approx_percentile(case when payment_type = 1 then tip_pct end, 0.9)   as p90_tip_pct_credit,
    sum(tip_amount)                                 as total_tip_amount
from enriched
group by 1, 2, 5, 6
