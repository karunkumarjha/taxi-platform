/*
================================================================================
audit_queries.sql — medallion-layer health snapshot
================================================================================

Manual cookbook for "what's actually in the platform right now?" — one
query per layer, runnable as ANALYST. The dbt test suite (97 automated
tests including assert_snapshot_current_uniqueness and
assert_monthly_row_counts_sane) is the actual validation; these queries
let you SEE the data, not just whether the assertions pass.

Per-business-question SQL lives in queries/01–04 (live marts) and
queries/05_historical_union.sql (live + Iceberg historical).
================================================================================
*/

USE ROLE ANALYST;
USE WAREHOUSE WH_XS;
USE DATABASE ANALYTICS;


-- ============================================================================
-- 1. Bronze (RAW.YELLOW_TRIPDATA) — load audit by pipeline source
-- ============================================================================
-- Append-only design with `_loaded_by` and `_ingest_batch_id`: every COPY
-- event is preserved, distinguishable, and timestamped.

SELECT
    _loaded_by,
    COUNT(*)                            AS rows_loaded,
    COUNT(DISTINCT _ingest_batch_id)    AS distinct_load_events,
    MIN(_loaded_at)                     AS first_load,
    MAX(_loaded_at)                     AS last_load
FROM RAW.YELLOW_TRIPDATA
GROUP BY _loaded_by
ORDER BY last_load DESC;


-- ============================================================================
-- 2. Silver (SNAPSHOTS.SNP_YELLOW_TRIPS) — SCD Type 2 health
-- ============================================================================
-- The snapshot has exactly one current row per trip_bk (invariant verified
-- by assert_snapshot_current_uniqueness). If TLC publishes corrections,
-- `trips_with_multiple_versions` > 0 — those are trips whose mutable
-- columns changed across loads.

SELECT
    COUNT(DISTINCT trip_bk)                                            AS unique_trips,
    COUNT(*)                                                           AS total_scd_rows,
    SUM(CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END)              AS current_versions,
    COUNT(DISTINCT CASE WHEN dbt_valid_to IS NOT NULL THEN trip_bk END) AS trips_with_multiple_versions
FROM SNAPSHOTS.SNP_YELLOW_TRIPS;


-- ============================================================================
-- 3. Gold atomic fact (MARTS.FCT_TRIPS) — top 10 zones by revenue
-- ============================================================================
-- Canonical trip-grain fact. Single row per valid trip; all 19 source
-- columns + zone enrichment + derived columns. The aggregate marts read
-- from here.

SELECT
    pu_borough,
    pu_zone,
    COUNT(*)                       AS trips,
    ROUND(SUM(total_amount), 0)    AS gross_revenue_usd,
    ROUND(AVG(total_amount), 2)    AS avg_fare_usd,
    ROUND(AVG(tip_pct) * 100, 1)   AS avg_tip_pct
FROM MARTS.FCT_TRIPS
GROUP BY pu_borough, pu_zone
ORDER BY gross_revenue_usd DESC
LIMIT 10;


-- ============================================================================
-- 4. Quarantine (MARTS.FCT_TRIPS_QUARANTINED) — invalid-row taxonomy
-- ============================================================================
-- Every dirty row that didn't make it to FCT_TRIPS, with its `invalid_reason`.
-- Platform invariant: every row in RAW.YELLOW_TRIPDATA appears in exactly
-- one of FCT_TRIPS or FCT_TRIPS_QUARANTINED — no silent drops.

SELECT
    invalid_reason,
    COUNT(*)                                AS rows_quarantined,
    ROUND(100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (), 3)           AS pct_of_quarantined
FROM MARTS.FCT_TRIPS_QUARANTINED
GROUP BY invalid_reason
ORDER BY rows_quarantined DESC;


-- ============================================================================
-- 5a. Data coverage — FCT_TRIPS by (year, month)
-- ============================================================================
-- Which months of TLC data have been ingested + built? One row per month
-- the pipeline has processed. Useful for confirming a backfill landed,
-- spotting gaps, and seeing the typical-month volume (~3M rows for NYC).

SELECT
    pickup_year,
    pickup_month,
    COUNT(*)                                                   AS valid_trip_count,
    ROUND(SUM(total_amount), 0)                                AS gross_revenue_usd,
    MIN(pickup_date)                                           AS first_pickup_date,
    MAX(pickup_date)                                           AS last_pickup_date
FROM MARTS.FCT_TRIPS
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;


-- ============================================================================
-- 5b. Data coverage — FCT_TRIPS_QUARANTINED by (year, month)
-- ============================================================================
-- Same shape for the quarantine table. Comparing 5a vs 5b per (year, month)
-- shows the valid:quarantined split per ingested month. Quarantine should
-- be <1% of valid; if a month skews high, inspect that month's
-- `invalid_reason` distribution by adding `WHERE pickup_year = … AND
-- pickup_month = …` to query 4 above.

SELECT
    pickup_year,
    pickup_month,
    COUNT(*)                                                   AS quarantined_trip_count,
    MIN(pickup_date)                                           AS first_pickup_date,
    MAX(pickup_date)                                           AS last_pickup_date
FROM MARTS.FCT_TRIPS_QUARANTINED
WHERE pickup_year IS NOT NULL                  -- null_timestamp rows have no year/month
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;
