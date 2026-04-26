{{
    config(
        materialized = 'table',
        alias        = 'fct_trips_quarantined',
    )
}}

-- Audit fact. Every row that failed staging's validity rules, with the reason.
--
-- Lives in `models/intermediate/` (it's a per-row pass-through of the
-- staging quarantine flag, not an aggregate), but exposed in MARTS as
-- `FCT_TRIPS_QUARANTINED` so analysts can audit dirty-record volumes
-- alongside the other facts.
--
-- Persisted as a table (not a view) so we can compute dirty-record stats per
-- month without re-scanning RAW.

select
    trip_sk,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    trip_distance,
    fare_amount,
    total_amount,
    payment_type,
    pu_location_id,
    do_location_id,
    invalid_reason,
    source_filename,
    loaded_at
from {{ ref('stg_yellow_trips') }}
where not is_valid
