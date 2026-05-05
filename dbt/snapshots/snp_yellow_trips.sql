{% snapshot snp_yellow_trips %}

{{
    config(
        target_database = env_var('SNOWFLAKE_DATABASE', 'ANALYTICS'),
        target_schema   = env_var('SNOWFLAKE_SNAPSHOTS_SCHEMA', 'SNAPSHOTS'),
        unique_key      = 'trip_bk',
        strategy        = 'check',
        check_cols      = [
            'fare_amount',
            'total_amount',
            'payment_type',
            'passenger_count',
            'trip_distance',
            'extra',
            'mta_tax',
            'tip_amount',
            'tolls_amount',
            'improvement_surcharge',
            'congestion_surcharge',
            'airport_fee',
            'cbd_congestion_fee',
            'store_and_fwd_flag',
            'ratecodeid',
        ],
    )
}}

-- Silver layer: SCD2 over Bronze RAW.
--
-- trip_bk = md5 of 5 immutable identifiers (vendor + pickup_ts + dropoff_ts +
-- pu/do_location_id). check_cols = all mutable financial / operational columns
-- TLC may retroactively correct. On a republish (RAW gets a new copy via
-- FORCE=TRUE), if any check_col differs the snapshot closes the old version
-- (dbt_valid_to = now) and opens a new one.
--
-- Within-run dedup (qualify row_number = 1): RAW is append-only and may have
-- both original + corrected rows for the same trip_bk in a single run, plus
-- occasional true duplicate rows from TLC. Snapshots require unique_key to
-- appear at most once per run, so we keep the most-recently-loaded row.
--
-- stg_yellow_trips reads WHERE dbt_valid_to IS NULL = current SCD version.

with raw_keyed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'VendorID',
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
        ]) }} as trip_bk,
        *
    from {{ source('raw', 'yellow_tripdata') }}
)

select
    trip_bk,
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
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
    _source_filename,
    _ingest_batch_id,
    _loaded_by,
    _loaded_at
from raw_keyed
qualify row_number() over (
    partition by trip_bk
    order by _loaded_at desc, _ingest_batch_id desc
) = 1

{% endsnapshot %}
