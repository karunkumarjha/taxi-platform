{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pu_location_id', 'distance_bucket', 'payment_type'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pu_location_id'],
    )
}}

-- Yearly rollup of agg_zone_tip_behaviour_monthly. Grain: (year, zone, bucket, payment).
-- No percentiles — p50/p90 aren't summable from monthly percentiles.
-- For yearly percentiles, query FCT_TRIPS directly (year-grain rebuild cost).

select
    pickup_year,
    pu_location_id,
    any_value(pu_borough)                                                                   as pu_borough,
    any_value(pu_zone)                                                                      as pu_zone,
    distance_bucket,
    payment_type,
    sum(trip_count)                                                                         as trip_count,
    sum(total_fare_amount)                                                                  as total_fare_amount,
    sum(total_tip_amount)                                                                   as total_tip_amount,
    sum(total_fare_amount) / nullif(sum(trip_count), 0)                                     as avg_fare,
    sum(total_tip_amount)  / nullif(sum(trip_count), 0)                                     as avg_tip,
    -- Trip-weighted yearly avg. Mart is keyed on payment_type so each
    -- row is one payment type — weighting by trip_count is correct per row.
    sum(avg_tip_pct_credit * trip_count) / nullif(sum(trip_count), 0)                       as avg_tip_pct_credit,
    current_timestamp()                                                                     as mart_built_at
from {{ ref('agg_zone_tip_behaviour_monthly') }}
{% if is_incremental() %}
where pickup_year = {{ var('target_year') }}
{% endif %}
group by 1, 2, 5, 6
