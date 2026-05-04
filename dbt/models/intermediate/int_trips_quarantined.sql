{{
    config(
        materialized        = 'incremental',
        unique_key          = 'quarantine_key',
        incremental_strategy= 'merge',
        on_schema_change    = 'sync_all_columns',
        alias               = 'fct_trips_quarantined',
    )
}}

-- Audit fact. Every row that failed staging's validity rules, with the
-- reason. All source columns + zone enrichment + invalid_reason preserved
-- so an investigator sees full context for any quarantined row.
--
-- unique_key = quarantine_key (surrogate, never NULL) because quarantined
-- rows include null_timestamp cases where natural key columns are NULL —
-- merge's equality check would be NULL-safe only with the surrogate.
--
-- Per-run month scoping via dbt vars (target_year / target_month):
--   On incremental runs, scopes to the target month. Rows with NULL
--   pickup_year (null_timestamp category) are always included — they
--   cannot be month-scoped so they're processed every run (volume is tiny).

with invalid_trips as (
    select t.*
    from {{ ref('stg_yellow_trips') }} t
    where not t.is_valid
    {% if is_incremental() %}
      and (
          t.pickup_year is null
          or (
              t.pickup_year  = {{ var('target_year') }}
              and t.pickup_month = {{ var('target_month') }}
          )
      )
    {% endif %}
),

zones as (
    select * from {{ ref('dim_zones') }}
),

keyed as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_ts', 'dropoff_ts', 'pu_location_id',
            'do_location_id', 'fare_amount', 'total_amount', 'source_filename'
        ]) }} as quarantine_key
    from invalid_trips
)

select
    t.quarantine_key,
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
from keyed t
left join zones pu  on pu.location_id  = t.pu_location_id
left join zones do_ on do_.location_id = t.do_location_id
