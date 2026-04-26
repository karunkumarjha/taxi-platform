{{
    config(
        materialized = 'table',
        cluster_by   = ['pickup_year', 'pu_location_id'],
    )
}}

-- Business Q4: What's the relationship between trip distance, payment type,
-- and tip percentage? Are there zones where passengers tip significantly
-- more or less?
--
-- Grain: (pickup_year, pu_location_id, distance_bucket, payment_type).
--
-- Key caveat, documented in the README: tip_amount is reliable only for
-- payment_type = 1 (credit). Cash tips (payment_type = 2) are NOT recorded by
-- the meter. A zone's "low tip" signal can be an artifact of cash-heavy
-- ridership rather than tipping behaviour. We keep all payment types in the
-- mart so the dashboard can expose this explicitly (filter to credit-only when
-- analysing actual tip %).

with enriched as (
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
    -- Filter to credit-card trips only for the tip percentage stats;
    -- everything else would be averaging into NULL/0.
    avg(case when payment_type = 1 then tip_pct end)                      as avg_tip_pct_credit,
    approx_percentile(case when payment_type = 1 then tip_pct end, 0.5)   as p50_tip_pct_credit,
    approx_percentile(case when payment_type = 1 then tip_pct end, 0.9)   as p90_tip_pct_credit,
    sum(tip_amount)                                 as total_tip_amount
from enriched
group by 1, 2, 5, 6
