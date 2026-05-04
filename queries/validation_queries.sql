USE ROLE ANALYST;
USE WAREHOUSE WH_XS;
USE DATABASE ANALYTICS;


-- ============================================================================
-- 1. Bronze (RAW.YELLOW_TRIPDATA) — load audit by pipeline source
-- ============================================================================
-- Shows the append-only design with `_loaded_by` and `_ingest_batch_id`:
-- every COPY event is preserved, distinguishable, and timestamped.

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
-- Shows the snapshot has exactly one current row per trip_bk (the
-- invariant verified by the assert_snapshot_current_uniqueness test).
-- If TLC ever publishes corrections, "trips_with_multiple_versions"
-- would be > 0 — those are the trips whose financial fields were
-- corrected over time.

SELECT
    COUNT(DISTINCT trip_bk)                                            AS unique_trips,
    COUNT(*)                                                           AS total_scd_rows,
    SUM(CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END)              AS current_versions,
    COUNT(DISTINCT CASE WHEN dbt_valid_to IS NOT NULL THEN trip_bk END) AS trips_with_multiple_versions
FROM SNAPSHOTS.SNP_YELLOW_TRIPS;


-- ============================================================================
-- 3. Gold atomic fact (MARTS.FCT_TRIPS) — top 10 pickup zones by revenue
-- ============================================================================
-- The canonical trip-grain fact. Single row per valid trip; all 19 source
-- columns + zone enrichment + derived columns. Production dashboards read
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
-- 4. Quarantine fact (MARTS.FCT_TRIPS_QUARANTINED) — invalid-row taxonomy
-- ============================================================================
-- Audit table. Every dirty row that didn't make it to FCT_TRIPS, with the
-- reason. The platform's "quarantine, never delete" guarantee — every row
-- in RAW lives in exactly one of FCT_TRIPS or FCT_TRIPS_QUARANTINED.

SELECT
    invalid_reason,
    COUNT(*)                                AS rows_quarantined,
    ROUND(100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (), 3)           AS pct_of_quarantined
FROM MARTS.FCT_TRIPS_QUARANTINED
GROUP BY invalid_reason
ORDER BY rows_quarantined DESC;


-- ============================================================================
-- Q1 — AGG_ZONE_REVENUE_MONTHLY: top 5 zones per month + rank movement
-- ============================================================================
-- Demonstrates the year-grain rank vs monthly-grain rank — useful for
-- "which zones surge or slip during peak months". LAG window picks up
-- month-over-month rank shift per zone.

SELECT
    pickup_year,
    pickup_month,
    pu_zone,
    ROUND(gross_revenue, 0)         AS revenue_usd,
    revenue_rank_in_month,
    yearly_revenue_rank,
    revenue_rank_in_month - LAG(revenue_rank_in_month) OVER (
        PARTITION BY pu_location_id
        ORDER BY pickup_year, pickup_month
    )                               AS rank_change_mom
FROM MARTS.AGG_ZONE_REVENUE_MONTHLY
QUALIFY revenue_rank_in_month <= 5
ORDER BY pickup_year, pickup_month, revenue_rank_in_month;


SELECT
    pickup_year,
    pickup_month,
    count(*)
FROM MARTS.fct_trips
group by 1,2;



-- ============================================================================
-- Q2 — AGG_HOURLY_DEMAND: peak hour for each day of week
-- ============================================================================
-- Demonstrates the hour × DOW × month grain. ROW_NUMBER + QUALIFY picks
-- the single peak hour per DOW. Pattern: weekday peaks ~6 PM, weekend
-- peaks shift to ~10-11 PM.

SELECT
    pickup_dow,
    pickup_hour,
    SUM(trip_count)                AS trips,
    ROUND(AVG(avg_fare), 2)        AS avg_fare_usd,
    ROUND(AVG(p50_fare), 2)        AS median_fare_usd
FROM MARTS.AGG_HOURLY_DEMAND
GROUP BY pickup_dow, pickup_hour
QUALIFY ROW_NUMBER() OVER (PARTITION BY pickup_dow ORDER BY trips DESC) = 1
ORDER BY pickup_dow;


-- ============================================================================
-- Q3 — AGG_ZONE_SUPPLY_GAPS: zones with the longest pickup gaps
-- ============================================================================
-- Operational-priority view: which zones sit silent for long stretches?
-- The 3h+ gap counter is the most actionable metric — "this zone went
-- N days with at least one 3-hour wait between consecutive pickups."

SELECT
    pu_zone,
    pu_borough,
    COUNT(*)                                AS days_observed,
    ROUND(AVG(longest_gap_min), 0)          AS avg_longest_daily_gap_min,
    MAX(longest_gap_min)                    AS worst_gap_observed_min,
    SUM(gaps_gt_3h)                         AS total_gaps_over_3h,
    ROUND(AVG(trip_count), 0)               AS avg_daily_trips
FROM MARTS.AGG_ZONE_SUPPLY_GAPS
GROUP BY pu_zone, pu_borough
HAVING days_observed >= 30
ORDER BY total_gaps_over_3h DESC
LIMIT 10;


-- ============================================================================
-- Q4 — AGG_ZONE_TIP_BEHAVIOUR: top tipping zones for credit-card trips
-- ============================================================================
-- Shows median tip% by (zone, distance_bucket, payment_type). Filtered to
-- credit-card (payment_type = 1) since cash tips aren't recorded. Ranking
-- by p50_tip_pct_credit reveals the "generous" zones at each distance.

SELECT
    pu_zone,
    pu_borough,
    distance_bucket,
    trip_count,
    ROUND(p50_tip_pct_credit * 100, 2)      AS median_tip_pct,
    ROUND(p90_tip_pct_credit * 100, 2)      AS p90_tip_pct,
    ROUND(avg_tip,             2)           AS avg_tip_usd
FROM MARTS.AGG_ZONE_TIP_BEHAVIOUR
WHERE payment_type = 1                      -- credit card only
  AND trip_count >= 1000                    -- statistically meaningful
ORDER BY median_tip_pct DESC
LIMIT 10;


-- ============================================================================
-- Iceberg historical (HISTORICAL.HISTORICAL_DAILY_AGG) — top zones across
-- all years. Read zero-copy from S3 via Glue catalog. Same SQL surface
-- as MARTS, different storage substrate.
-- ============================================================================
-- This table is empty until spark_historical has been triggered for at
-- least one year. After running for 2023 / 2022 / etc., this query
-- returns historical patterns the live dbt path can't see efficiently.

SELECT
  YEAR(pickup_date)                          AS year,
  pu_location_id,
  SUM(trip_count)                            AS total_trips,
  ROUND(SUM(gross_revenue), 0)               AS revenue_usd,
  ROUND(SUM(total_tip), 0)                   AS total_tips_usd,
  ROUND(SUM(total_distance_mi), 0)           AS distance_mi,
  ROUND(SUM(total_duration_sec) / 3600.0, 0) AS hours_on_road
FROM HISTORICAL.HISTORICAL_DAILY_AGG
GROUP BY YEAR(pickup_date), pu_location_id
ORDER BY revenue_usd DESC
LIMIT 10;


-- ============================================================================
-- Bonus: union live + historical for a full multi-year time series
-- ============================================================================
-- Demonstrates the open-table-format payoff: one SQL query reads from
-- BOTH a Snowflake-native table (MARTS.AGG_ZONE_REVENUE_MONTHLY) and
-- an Iceberg table on S3 (HISTORICAL.HISTORICAL_DAILY_AGG via Glue
-- catalog). Same role, same SQL, zero data duplication.

  SELECT 'live'        AS source,
         pickup_year   AS year,
         pickup_month  AS month,
         SUM(trip_count) AS trips,
         ROUND(SUM(gross_revenue), 0) AS revenue_usd
  FROM MARTS.AGG_ZONE_REVENUE_MONTHLY
  GROUP BY 1, 2, 3
  UNION ALL
  SELECT 'historical',
         YEAR(pickup_date)  AS year,
         MONTH(pickup_date) AS month,
         SUM(trip_count),
         ROUND(SUM(gross_revenue), 0)
  FROM HISTORICAL.HISTORICAL_DAILY_AGG
  GROUP BY 1, 2, 3
  ORDER BY year, month, source;
