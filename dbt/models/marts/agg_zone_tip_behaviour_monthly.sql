{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pickup_month', 'pu_location_id', 'distance_bucket', 'payment_type'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pickup_month', 'pu_location_id'],
    )
}}

-- Q4: tip % by (year, month, zone, distance_bucket, payment_type).
-- Percentiles (p50/p90 tip_pct) computed at month grain; they aren't
-- summable so the yearly mart only carries trip-weighted averages.
--
-- Caveat: tip_amount is reliable only for payment_type=1 (credit) — cash
-- tips aren't recorded by the meter. All payment types are kept so the
-- dashboard can filter to credit-only explicitly.

with enriched as (
    select
        pickup_year,
        pickup_month,
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
    {% if is_incremental() %}
    where pickup_year  = {{ var('target_year') }}
      and pickup_month = {{ var('target_month') }}
    {% endif %}
)

select
    pickup_year,
    pickup_month,
    pu_location_id,
    any_value(pu_borough)                           as pu_borough,
    any_value(pu_zone)                              as pu_zone,
    distance_bucket,
    payment_type,
    count(*)                                        as trip_count,
    sum(fare_amount)                                as total_fare_amount,
    sum(tip_amount)                                 as total_tip_amount,
    avg(fare_amount)                                as avg_fare,
    avg(tip_amount)                                 as avg_tip,
    avg(case when payment_type = 1 then tip_pct end)                      as avg_tip_pct_credit,
    approx_percentile(case when payment_type = 1 then tip_pct end, 0.5)   as p50_tip_pct_credit,
    approx_percentile(case when payment_type = 1 then tip_pct end, 0.9)   as p90_tip_pct_credit,
    current_timestamp()                             as mart_built_at
from enriched
group by 1, 2, 3, 6, 7
