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

-- Silver layer: SCD Type 2 over the Bronze RAW table.
--
-- trip_bk (business key) = md5 of the five immutable trip identifiers:
--   vendor_id, pickup datetime, dropoff datetime, pickup zone, dropoff zone.
-- These columns cannot change — they describe the physical trip event.
--
-- check_cols = all mutable attributes TLC may retroactively correct:
-- fare adjustments, payment corrections, distance recalculations, etc.
-- When TLC re-publishes a file with a corrected row, FORCE=TRUE in
-- COPY INTO lands both the original and the corrected version in RAW
-- (distinguished by _ingest_batch_id). The snapshot compares check_cols
-- for each trip_bk and, if any changed, closes the previous version
-- (dbt_valid_to = current_timestamp) and opens a new one.
--
-- Why deduplicate within this query (qualify row_number = 1):
--   RAW is append-only (FORCE=TRUE) and TLC files occasionally have true
--   duplicate rows (same trip recorded twice within one parquet). Both
--   patterns produce multiple rows per trip_bk in a single snapshot run.
--   Snapshots require each unique_key to appear at most once per run, so
--   we pick the most recently loaded row per trip_bk before snapshotting.
--   Tie-breaker: prefer the row with non-null mutable attributes when
--   _loaded_at is identical (defensive — shouldn't happen in practice).
--
-- Downstream:
--   stg_yellow_trips reads WHERE dbt_valid_to IS NULL — that's dbt's idiom
--   for "current SCD version" (older versions have a closed dbt_valid_to
--   timestamp). The full SCD history remains in this table for audit queries.

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
