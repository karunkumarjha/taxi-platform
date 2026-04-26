{{
    config(
        materialized        = 'incremental',
        unique_key          = 'trip_sk',
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        alias               = 'fct_trips_quarantined',
    )
}}

-- Audit fact. Every row that failed staging's validity rules, with the
-- reason. All 19 source columns + zone enrichment + invalid_reason are
-- preserved so an investigator looking at a quarantined row sees the same
-- context they'd see in FCT_TRIPS.
--
-- Lives in `models/intermediate/` (it's a per-row pass-through of the
-- staging quarantine flag, not an aggregate), but exposed in MARTS as
-- `FCT_TRIPS_QUARANTINED` so analysts can audit dirty-record volumes
-- alongside the other facts.
--
-- Why unique_key = trip_sk (not natural-key tuple like FCT_TRIPS):
--   Quarantined rows include `null_timestamp` cases where pickup_ts /
--   dropoff_ts are NULL — the natural-key tuple would have NULLs that
--   dbt's `delete+insert` (NULL-unsafe equality) can't dedupe. trip_sk
--   uses dbt_utils' null sentinel, so it's never NULL. Volume of
--   quarantined rows is <1% of total, so the cross-engine drift
--   (Spark-computed vs dbt-computed trip_sk for the same physical row)
--   is negligible in practice. Documented asymmetry, not a bug.
--
-- Self-healing incremental: same count-divergence pattern as
-- int_trips_enriched.

with stg_invalid_month_counts as (
    select pickup_year, pickup_month, count(*) as stg_cnt
    from {{ ref('stg_yellow_trips') }}
    where not is_valid
      and pickup_year is not null
    group by 1, 2
),

months_to_build as (
    select s.pickup_year, s.pickup_month
    from stg_invalid_month_counts s
    {% if is_incremental() %}
    left join (
        select pickup_year, pickup_month, count(*) as q_cnt
        from {{ this }}
        where pickup_year is not null
        group by 1, 2
    ) q using (pickup_year, pickup_month)
    where coalesce(q.q_cnt, 0) != s.stg_cnt
    {% endif %}
),

invalid_trips as (
    select t.*
    from {{ ref('stg_yellow_trips') }} t
    where not t.is_valid
      and (
          t.pickup_year is null
          or (t.pickup_year, t.pickup_month) in (
              select pickup_year, pickup_month from months_to_build
          )
      )
),

zones as (
    select * from {{ ref('dim_zones') }}
)

select
    t.trip_sk,
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
    t.invalid_reason,
    t.source_filename,
    t.loaded_at
from invalid_trips t
left join zones pu  on pu.location_id  = t.pu_location_id
left join zones do_ on do_.location_id = t.do_location_id
