{{
    config(
        materialized        = 'incremental',
        unique_key          = 'trip_bk',
        incremental_strategy= 'merge',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_date', 'pu_location_id'],
        alias               = 'fct_trips',
    )
}}

-- Atomic trip-grain fact. One row per VALID taxi trip — the canonical
-- source of truth that every aggregate mart builds from.
--
-- UNIQUE_KEY = trip_bk (surrogate of the 5 immutable trip identifiers).
-- INCREMENTAL_STRATEGY = merge: when TLC corrects a row (e.g. fare
-- adjustment), the snapshot closes the old SCD version and opens a new
-- one. Staging reflects the correction. The merge here upserts the
-- corrected row into this table — no stale data survives.
--
-- Per-run month scoping via dbt vars (set by Airflow):
--   target_year / target_month — the single month being processed.
--   On the first full run (table doesn't exist), the is_incremental()
--   block is skipped so ALL valid trips are loaded.
--
-- Cluster on (pickup_date, pu_location_id):
--   Every aggregate mart and ad-hoc query filters on date and/or
--   pu_location_id. Snowflake micro-partition pruning kicks in cleanly.

with trips as (
    select t.*
    from {{ ref('stg_yellow_trips') }} t
    where t.is_valid
    {% if is_incremental() %}
      and t.pickup_year  = {{ var('target_year') }}
      and t.pickup_month = {{ var('target_month') }}
    {% endif %}
),

zones as (
    select * from {{ ref('dim_zones') }}
)

select
    t.trip_bk,
    t.vendor_id,
    t.pickup_ts,
    t.dropoff_ts,
    t.pickup_date,
    t.pickup_hour,
    t.pickup_dow,
    t.pickup_year,
    t.pickup_month,
    t.trip_duration_min,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.tip_pct,
    t.pu_location_id,
    pu.borough        as pu_borough,
    pu.zone_name      as pu_zone,
    pu.service_zone   as pu_service_zone,
    t.do_location_id,
    do_.borough       as do_borough,
    do_.zone_name     as do_zone,
    do_.service_zone  as do_service_zone,
    t.source_filename,
    t.loaded_at
from trips t
left join zones pu  on pu.location_id  = t.pu_location_id
left join zones do_ on do_.location_id = t.do_location_id
