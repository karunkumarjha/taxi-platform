{{
    config(
        materialized = 'view',
    )
}}

-- Staging for the TLC Yellow Taxi trips.
--
-- Design choices:
--   • Stays a view — 1:1 reflection of RAW. Per-month scoping happens in the
--     downstream incremental models (int_trips_enriched, int_trips_quarantined),
--     not here, so the view's definition is independent of any single run.
--   • Cast + rename at the boundary so the rest of dbt uses consistent names
--     (pickup_ts, dropoff_ts, pickup_date, pickup_hour, pickup_dow).
--   • Do NOT drop invalid rows here. Tag them with is_valid + invalid_reason so
--     downstream layers can split cleanly — int_trips_enriched keeps only valid
--     rows, int_trips_quarantined keeps the rest as an audit trail.
--   • Validity rules match the README "dirty records" section. Order matters:
--     rules are evaluated top-down and the first failure sets invalid_reason.

with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

casted as (
    select
        vendorid::integer                      as vendor_id,
        tpep_pickup_datetime::timestamp_ntz    as pickup_ts,
        tpep_dropoff_datetime::timestamp_ntz   as dropoff_ts,
        passenger_count::integer               as passenger_count,
        trip_distance::float                   as trip_distance,
        ratecodeid::integer                    as ratecode_id,
        store_and_fwd_flag                     as store_and_fwd_flag,
        pulocationid::integer                  as pu_location_id,
        dolocationid::integer                  as do_location_id,
        payment_type::integer                  as payment_type,
        fare_amount::float                     as fare_amount,
        extra::float                           as extra,
        mta_tax::float                         as mta_tax,
        tip_amount::float                      as tip_amount,
        tolls_amount::float                    as tolls_amount,
        improvement_surcharge::float           as improvement_surcharge,
        total_amount::float                    as total_amount,
        congestion_surcharge::float            as congestion_surcharge,
        airport_fee::float                     as airport_fee,
        cbd_congestion_fee::float              as cbd_congestion_fee,
        _source_filename                       as source_filename,
        _loaded_at                             as loaded_at
    from source
),

derived as (
    select
        *,
        -- Use second precision and convert to fractional minutes so legitimate
        -- sub-minute trips (pickup 12:00:30, dropoff 12:00:50) come through as
        -- ~0.33 instead of 0. The minute-floored version was wrongly tripping
        -- the trip_duration_sane test on ~7k January rows.
        datediff('second', pickup_ts, dropoff_ts) / 60.0          as trip_duration_min,
        date_trunc('day', pickup_ts)::date                       as pickup_date,
        extract(hour from pickup_ts)::integer                    as pickup_hour,
        -- ISO day of week: 1=Monday ... 7=Sunday. Stable across Snowflake sessions.
        extract(dayofweekiso from pickup_ts)::integer            as pickup_dow,
        extract(year from pickup_ts)::integer                    as pickup_year,
        extract(month from pickup_ts)::integer                   as pickup_month,
        case
            when fare_amount > 0 then tip_amount / fare_amount
            else null
        end                                                       as tip_pct,
        -- Detect duplicate raw rows. TLC's source data contains a small
        -- number of true duplicates (vendor recorded the same trip twice).
        -- We pick the first occurrence (by source_filename, deterministic)
        -- and quarantine the rest as 'duplicate_row'.
        row_number() over (
            partition by
                vendor_id, pickup_ts, dropoff_ts,
                pu_location_id, do_location_id,
                fare_amount, total_amount, payment_type
            order by source_filename
        )                                                         as dup_rank
    from casted
),

flagged as (
    select
        *,
        case
            when pickup_ts is null or dropoff_ts is null           then 'null_timestamp'
            when pickup_ts >= dropoff_ts                           then 'pickup_ge_dropoff'
            -- Trips longer than 12h are physically implausible for an NYC
            -- taxi ride — almost always meters left running overnight or a
            -- meter glitch. Same threshold as the trip_duration_sane test.
            when trip_duration_min > 720                           then 'duration_out_of_range'
            when trip_distance is null or trip_distance <= 0       then 'non_positive_distance'
            when trip_distance > 200                               then 'distance_out_of_range'
            when fare_amount < 0 or total_amount < 0               then 'negative_fare_or_total'
            -- Implausibly large fares are meter glitches. The most expensive
            -- legit NYC yellow trips are <$500 (long-distance + surge); $1k
            -- is well past any real-world ceiling. Same pattern as
            -- duration_out_of_range.
            when fare_amount > 1000 or total_amount > 1000         then 'excessive_fare_or_total'
            -- Tip > fare is suspicious; tip > total is mathematically
            -- impossible (total includes tip), always a data bug.
            when tip_amount > fare_amount or tip_amount > total_amount
                                                                   then 'tip_exceeds_fare_or_total'
            when payment_type not in (1, 2, 3, 4, 5, 6)            then 'unknown_payment_type'
            when pu_location_id is null or do_location_id is null  then 'null_location_id'
            when dup_rank > 1                                      then 'duplicate_row'
            else null
        end as invalid_reason
    from derived
)

select
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id', 'pickup_ts', 'dropoff_ts', 'pu_location_id',
        'do_location_id', 'fare_amount', 'total_amount', 'source_filename'
    ]) }}                                      as trip_sk,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    pickup_hour,
    pickup_dow,
    pickup_year,
    pickup_month,
    trip_duration_min,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    tip_pct,
    invalid_reason,
    invalid_reason is null                     as is_valid,
    source_filename,
    loaded_at
from flagged
