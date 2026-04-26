{{
    config(
        materialized = 'table',
        cluster_by   = ['pickup_date', 'pu_location_id'],
        alias        = 'fct_trips',
    )
}}

-- Atomic trip-grain fact. One row per VALID taxi trip.
--
-- Lives in `models/intermediate/` (logical layer between staging cleansing and
-- the aggregate marts), but is exposed in MARTS as `FCT_TRIPS` via `+alias` so
-- analysts and dashboards see a stable canonical-fact name in the consumer
-- schema.
--
-- Built from stg_yellow_trips (filtered to is_valid) + dim_zones.
--
-- Cluster on (pickup_date, pu_location_id):
--   Every aggregate mart and ad-hoc query filters on date and/or pu_location_id.
--   These are the two highest-cardinality predicates, ordered by the typical
--   filter pattern. Snowflake's micro-partition pruning kicks in cleanly here.

with trips as (
    select *
    from {{ ref('stg_yellow_trips') }}
    where is_valid
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
    t.payment_type,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
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
