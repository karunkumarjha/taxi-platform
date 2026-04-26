{{
    config(
        materialized        = 'incremental',
        unique_key          = [
            'vendor_id', 'pickup_ts', 'dropoff_ts',
            'pu_location_id', 'do_location_id',
            'fare_amount', 'total_amount', 'payment_type',
        ],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_date', 'pu_location_id'],
        alias               = 'fct_trips',
    )
}}

-- Atomic trip-grain fact. One row per VALID taxi trip — the canonical
-- source of truth that every aggregate mart builds from. All 19 source
-- columns from RAW.YELLOW_TRIPDATA are preserved (cast + renamed only,
-- not dropped), plus derived columns (pickup_date / hour / dow / year /
-- month, trip_duration_min, tip_pct), zone enrichment (pu_borough,
-- pu_zone, etc.), and load metadata (source_filename, loaded_at).
--
-- Lives in `models/intermediate/` (logical layer between staging
-- cleansing and the aggregate marts), but is exposed in MARTS as
-- `FCT_TRIPS` via `+alias` so analysts see a stable canonical-fact name.
--
-- UNIQUE_KEY = natural-key tuple, NOT trip_sk:
--   Eight columns that uniquely identify a real trip
--   (vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id,
--    fare_amount, total_amount, payment_type) — the same combination
--   stg's dup_rank window partitions on.
--
--   Why not trip_sk? trip_sk is a surrogate (md5 hash of stringified
--   columns). The Spark and dbt implementations compute it independently,
--   and engine-specific cast-to-string formatting (e.g. timestamps with
--   vs without millisecond precision, NUMBER vs DOUBLE string repr)
--   produces DIFFERENT trip_sk values for the same physical trip. Using
--   trip_sk as unique_key would defeat the delete+insert dedup whenever
--   Spark and dbt both wrote the same month — silent double-counting.
--   Natural keys come straight from the source columns and ARE
--   bit-identical across engines.
--
--   trip_sk stays in the schema as a convenient single-column identifier
--   for downstream joins and analyst convenience. It's no longer
--   load-bearing for correctness.
--
-- SELF-HEALING INCREMENTAL via count-divergence:
--   The detection CTE compares per-(year, month) row counts in stg
--   (filtered to is_valid) against this table. Months where counts
--   diverge get rebuilt; matching months are skipped. Handles new live
--   months AND late-arriving cross-month leak rows merging into existing
--   months, without any vars or run parameters.
--
-- Cluster on (pickup_date, pu_location_id):
--   Every aggregate mart and ad-hoc query filters on date and/or
--   pu_location_id. These are the two highest-cardinality predicates,
--   ordered by typical filter pattern. Snowflake's micro-partition
--   pruning kicks in cleanly here.

with stg_month_counts as (
    select pickup_year, pickup_month, count(*) as stg_cnt
    from {{ ref('stg_yellow_trips') }}
    where is_valid
    group by 1, 2
),

months_to_build as (
    select s.pickup_year, s.pickup_month
    from stg_month_counts s
    {% if is_incremental() %}
    left join (
        select pickup_year, pickup_month, count(*) as fct_cnt
        from {{ this }}
        group by 1, 2
    ) f using (pickup_year, pickup_month)
    where coalesce(f.fct_cnt, 0) != s.stg_cnt
    {% endif %}
),

trips as (
    select t.*
    from {{ ref('stg_yellow_trips') }} t
    where t.is_valid
      and (t.pickup_year, t.pickup_month) in (
          select pickup_year, pickup_month from months_to_build
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
    t.source_filename,
    t.loaded_at
from trips t
left join zones pu  on pu.location_id  = t.pu_location_id
left join zones do_ on do_.location_id = t.do_location_id
