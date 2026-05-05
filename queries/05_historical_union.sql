/*
================================================================================
Q1–Q4 across full history — live dbt marts UNION historical Iceberg tables
================================================================================

Purpose
-------
Each of the four business questions, answered across the full available
range. The live dbt marts (`MARTS.AGG_*_MONTHLY` / `_YEARLY` / `_DAILY`)
hold whatever months `dbt_pipeline` has built; the Iceberg tables in
`HISTORICAL.*` hold whatever years `spark_historical` has run for. A
single SQL surface (Snowflake) reads both — the dbt marts as native
tables, the Iceberg tables zero-copy via CATALOG INTEGRATION to AWS
Glue. Same role (ANALYST), same query shape, no data duplication.

Source coverage
---------------
  • Live (dbt-built):      MARTS.AGG_*  (post-refactor names; see queries/01-04)
  • Historical (Spark):    HISTORICAL.DAILY_AGG          → Q1, Q2
                           HISTORICAL.SUPPLY_GAPS        → Q3
                           HISTORICAL.TIP_BEHAVIOUR      → Q4

The historical tables are populated by triggering the `spark_historical`
DAG with a `year` Param — re-runs of the same year are idempotent.
Empty-but-queryable until at least one year has been processed.

Performance
-----------
  • Live marts: pre-aggregated (~thousands of rows). Sub-second.
  • Historical: Iceberg's `months(pickup_date)` hidden partitioning
    means a year/month predicate gets pruning. Even at 1.5B raw rows,
    the historical tables are small post-aggregation.
  • UNION ALL avoids the dedup pass that UNION DISTINCT would force.
================================================================================
*/

USE ROLE ANALYST; USE WAREHOUSE WH_XS; USE DATABASE ANALYTICS;


-- ============================================================================
-- Q1 — Zone revenue + monthly rank, per year
-- ============================================================================
-- Roll the historical daily-grain table to monthly grain so it lines up
-- with AGG_ZONE_REVENUE_MONTHLY. Rank within each (year, month) across
-- all sources combined.

WITH live_monthly AS (
    SELECT pickup_year, pickup_month, pu_location_id,
           SUM(trip_count)    AS trip_count,
           SUM(gross_revenue) AS gross_revenue
    FROM MARTS.AGG_ZONE_REVENUE_MONTHLY
    GROUP BY 1, 2, 3
),
historical_monthly AS (
    SELECT YEAR(pickup_date)  AS pickup_year,
           MONTH(pickup_date) AS pickup_month,
           pu_location_id,
           SUM(trip_count)    AS trip_count,
           SUM(gross_revenue) AS gross_revenue
    FROM HISTORICAL.DAILY_AGG
    GROUP BY 1, 2, 3
),
combined AS (
    SELECT 'live'       AS source, * FROM live_monthly
    UNION ALL
    SELECT 'historical' AS source, * FROM historical_monthly
)
SELECT
    pickup_year, pickup_month, pu_location_id,
    SUM(trip_count)            AS trip_count,
    ROUND(SUM(gross_revenue),0) AS revenue_usd,
    RANK() OVER (
        PARTITION BY pickup_year, pickup_month
        ORDER BY SUM(gross_revenue) DESC
    )                          AS revenue_rank_in_month,
    LISTAGG(DISTINCT source, ',') WITHIN GROUP (ORDER BY source) AS sources
FROM combined
GROUP BY 1, 2, 3
QUALIFY revenue_rank_in_month <= 20
ORDER BY pickup_year, pickup_month, revenue_rank_in_month;


-- ============================================================================
-- Q2 — Hour × DOW demand pattern across the full range
-- ============================================================================
-- The historical daily table has pickup_hour but no dow — derive at query
-- time. Live mart has both grains directly.

WITH live_hourly AS (
    SELECT pickup_year, pickup_dow, pickup_hour,
           trip_count,
           avg_fare * trip_count AS weighted_fare_sum
    FROM MARTS.AGG_HOURLY_DEMAND_MONTHLY
),
historical_hourly AS (
    SELECT YEAR(pickup_date)        AS pickup_year,
           DAYOFWEEKISO(pickup_date) AS pickup_dow,
           pickup_hour,
           trip_count,
           total_fare AS weighted_fare_sum
    FROM HISTORICAL.DAILY_AGG
),
combined AS (
    SELECT * FROM live_hourly
    UNION ALL
    SELECT * FROM historical_hourly
)
SELECT
    pickup_year,
    pickup_dow,
    pickup_hour,
    SUM(trip_count)                                                  AS trip_count,
    ROUND(SUM(weighted_fare_sum) / NULLIF(SUM(trip_count), 0), 2)    AS avg_fare_usd
FROM combined
GROUP BY 1, 2, 3
ORDER BY pickup_year, pickup_dow, pickup_hour;


-- ============================================================================
-- Q3 — Underserved zones across all years
-- ============================================================================
-- Both sources are at (date, zone) grain — direct UNION. Roll up to
-- (year, zone) to surface chronically under-served zones across the
-- whole window.

WITH combined AS (
    SELECT pickup_date, pu_location_id, trip_count,
           longest_gap_min, gaps_gt_1h, gaps_gt_3h
    FROM MARTS.AGG_ZONE_SUPPLY_GAPS_DAILY
    UNION ALL
    SELECT pickup_date, pu_location_id, trip_count,
           longest_gap_min, gaps_gt_1h, gaps_gt_3h
    FROM HISTORICAL.SUPPLY_GAPS
)
SELECT
    YEAR(pickup_date)                AS pickup_year,
    pu_location_id,
    COUNT(DISTINCT pickup_date)      AS days_with_trips,
    SUM(trip_count)                  AS total_trips,
    ROUND(MAX(longest_gap_min), 0)   AS longest_gap_min_year,
    ROUND(AVG(longest_gap_min), 1)   AS avg_longest_gap_per_day,
    COUNT_IF(gaps_gt_1h > 0)         AS days_with_gap_over_60min,
    COUNT_IF(gaps_gt_3h > 0)         AS days_with_gap_over_3h,
    RANK() OVER (
        PARTITION BY YEAR(pickup_date)
        ORDER BY COUNT_IF(gaps_gt_3h > 0) DESC, AVG(longest_gap_min) DESC
    )                                AS underserved_rank
FROM combined
WHERE pu_location_id IS NOT NULL
GROUP BY 1, 2
HAVING days_with_trips >= 30 AND total_trips >= 1000
QUALIFY underserved_rank <= 20
ORDER BY pickup_year, underserved_rank;


-- ============================================================================
-- Q4 — Tip behaviour by distance × payment × zone, per year
-- ============================================================================
-- Both sources at (zone, distance_bucket, payment_type) grain when rolled
-- to year. Trip-weighted yearly average tip percentage; credit-card only
-- since cash tips aren't recorded by the meter.

WITH live_yearly AS (
    SELECT pickup_year, pu_location_id, distance_bucket, payment_type,
           trip_count,
           total_fare_amount AS total_fare,
           total_tip_amount  AS total_tip
    FROM MARTS.AGG_ZONE_TIP_BEHAVIOUR_MONTHLY
),
historical_yearly AS (
    SELECT YEAR(pickup_date) AS pickup_year,
           pu_location_id, distance_bucket, payment_type,
           trip_count, total_fare, total_tip
    FROM HISTORICAL.TIP_BEHAVIOUR
),
combined AS (
    SELECT * FROM live_yearly
    UNION ALL
    SELECT * FROM historical_yearly
)
SELECT
    pickup_year,
    pu_location_id,
    distance_bucket,
    payment_type,
    SUM(trip_count)                                                            AS trip_count,
    ROUND(SUM(total_tip) / NULLIF(SUM(total_fare), 0) * 100, 2)                AS avg_tip_pct,
    ROUND(SUM(total_tip) / NULLIF(SUM(trip_count), 0), 2)                      AS avg_tip_usd
FROM combined
WHERE payment_type = 1                       -- credit card; cash tips unrecorded
GROUP BY 1, 2, 3, 4
HAVING SUM(trip_count) >= 1000               -- sample-size floor
ORDER BY pickup_year, distance_bucket, avg_tip_pct DESC;
