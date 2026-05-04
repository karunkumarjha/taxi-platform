/*
================================================================================
Business question (Q4): What's the relationship between trip distance, payment
type, and tip percentage? Are there zones where passengers tip significantly
more or less?
================================================================================

Approach
--------
1. Bucket every trip by distance (<=1mi, 1-3mi, ..., 20mi+).
2. Compute tip_pct = tip_amount / fare_amount (credit-card trips only, because
   cash tips are not recorded on the meter — a critical caveat called out in
   the README's data-quality section).
3. Aggregate by (pu_location_id, distance_bucket) with p50/p90 via
   APPROX_PERCENTILE, and compare each zone's p50 to the city-wide p50 using
   a window function — lets us flag "zones that tip >2pp above / below average".
4. Filter to zones with a statistically meaningful sample
   (>= 1000 credit-card trips in the bucket).

SQL features used
-----------------
* Distance bucketing via CASE (WIDTH_BUCKET is an alternative — CASE is
  clearer for named buckets).
* Window function to compare per-zone p50 to the global p50 (deviation).
* APPROX_PERCENTILE for tip_pct distribution.
* Final WHERE on `trip_count` drops low-volume zones from the output.

Performance on Snowflake (38M rows)
-----------------------------------
* Filter to credit-only (payment_type = 1) upfront — ~62% of rows. Predicate
  pushdown + cluster by pickup_date means Snowflake scans only ~60% of
  micro-partitions, and within those reads fewer rows.
* The main cost is APPROX_PERCENTILE across ~23M credit rows grouped by
  ~265 zones × 6 buckets (~1.6k groups). Still cheap on XS.
* For a Snowsight dashboard, route to mart `fct_zone_tip_behaviour` instead.
================================================================================
*/

WITH credit_trips AS (
    SELECT
        pu_location_id,
        pu_borough,
        pu_zone,
        tip_amount,
        fare_amount,
        tip_amount / NULLIF(fare_amount, 0) AS tip_pct,
        CASE
            WHEN trip_distance <= 1   THEN '0-1mi'
            WHEN trip_distance <= 3   THEN '1-3mi'
            WHEN trip_distance <= 5   THEN '3-5mi'
            WHEN trip_distance <= 10  THEN '5-10mi'
            WHEN trip_distance <= 20  THEN '10-20mi'
            ELSE                           '20mi+'
        END AS distance_bucket
    FROM ANALYTICS.MARTS.FCT_TRIPS
    WHERE pickup_year = 2023
      AND payment_type = 1      -- credit card; cash tips are not recorded
      AND fare_amount > 0        -- avoid divide-by-zero on tip_pct
),

bucket_stats AS (
    SELECT
        pu_location_id,
        ANY_VALUE(pu_borough)                        AS pu_borough,
        ANY_VALUE(pu_zone)                           AS pu_zone,
        distance_bucket,
        COUNT(*)                                     AS trip_count,
        AVG(tip_pct)                                 AS avg_tip_pct,
        APPROX_PERCENTILE(tip_pct, 0.5)              AS p50_tip_pct,
        APPROX_PERCENTILE(tip_pct, 0.9)              AS p90_tip_pct
    FROM credit_trips
    GROUP BY 1, 4
),

with_baseline AS (
    SELECT
        *,
        -- Global p50 for this distance bucket (no zone split) — lets us
        -- say "zone X tips Y percentage points above/below the city norm".
        AVG(p50_tip_pct) OVER (PARTITION BY distance_bucket)    AS city_avg_p50_tip_pct,
        p50_tip_pct - AVG(p50_tip_pct) OVER (PARTITION BY distance_bucket)
                                                                AS p50_tip_pct_deviation
    FROM bucket_stats
)

SELECT
    pu_location_id,
    pu_borough,
    pu_zone,
    distance_bucket,
    trip_count,
    ROUND(avg_tip_pct * 100, 2)               AS avg_tip_pct,
    ROUND(p50_tip_pct * 100, 2)               AS p50_tip_pct,
    ROUND(p90_tip_pct * 100, 2)               AS p90_tip_pct,
    ROUND(city_avg_p50_tip_pct * 100, 2)      AS city_avg_p50_tip_pct,
    ROUND(p50_tip_pct_deviation * 100, 2)     AS p50_tip_pct_deviation_pp
FROM with_baseline
WHERE trip_count >= 1000
ORDER BY distance_bucket, p50_tip_pct_deviation DESC;
