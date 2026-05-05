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
-- appear at most once per run.
--
-- Tiebreak ordering — content-aware so the deterministic dedup pick aligns
-- with Spark's process_historical.py (same WHEN-priorities, same final
-- columns). Both pipelines deterministically prefer the copy MOST LIKELY
-- to pass the stg_yellow_trips validity gate, so a TLC duplicate where
-- one copy has payment_type=0 and the other has payment_type=1 always
-- collapses to the legitimate copy. Without this alignment, the two
-- engines' default tiebreaks (Snowflake micro-partition order vs Spark
-- DataFrame partition order) pick different copies on mixed-validity
-- pairs, producing systematic count drift between MARTS.AGG_* and
-- HISTORICAL.* — see commit history.
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
    order by
        -- Latest-load-first stays the primary key (handles TLC republishes).
        _loaded_at desc,
        _ingest_batch_id desc,
        -- Content-aware tiebreak: prefer the copy that would pass stg
        -- validity. 0 sorts before 1, so "0 = passes rule" wins.
        case when payment_type in (1, 2, 3, 4, 5, 6) then 0 else 1 end,
        case when fare_amount  >= 0 and fare_amount  <= 1000 then 0 else 1 end,
        case when total_amount >= 0 and total_amount <= 1000 then 0 else 1 end,
        case when trip_distance > 0 and trip_distance <= 200 then 0 else 1 end,
        case when tip_amount <= fare_amount and tip_amount <= total_amount then 0 else 1 end,
        -- Final stable tiebreak — column order MUST match
        -- spark/process_historical.py's dedupe_natural_key ladder
        -- column-for-column so the only case that falls back to
        -- engine-internal order is when two rows are byte-identical
        -- (where the pick genuinely doesn't matter).
        payment_type            nulls last,
        fare_amount             nulls last,
        tip_amount              nulls last,
        total_amount            nulls last,
        passenger_count         nulls last,
        trip_distance           nulls last,
        extra                   nulls last,
        mta_tax                 nulls last,
        tolls_amount            nulls last,
        improvement_surcharge   nulls last,
        congestion_surcharge    nulls last,
        airport_fee             nulls last,
        cbd_congestion_fee      nulls last,
        ratecodeid              nulls last,
        store_and_fwd_flag      nulls last
) = 1

{% endsnapshot %}
