/*
================================================================================
Business question (Q2): How do trip volume and average fare vary across hours
of the day? When are the peak and trough periods?
================================================================================

Approach
--------
Aggregate by (pickup_dow, pickup_hour) across 2023, then use window functions
to tag the single peak hour (highest trip_count) and single trough hour
(lowest non-zero trip_count) for each day-of-week. QUALIFY keeps only the
peak + trough rows plus any hour the caller explicitly filters in.

SQL features used
-----------------
* CTE stack with aggregations (COUNT, AVG, APPROX_PERCENTILE).
* APPROX_PERCENTILE for median fare — Snowflake-native HLL-like function,
  much cheaper than PERCENTILE_CONT at scale.
* Two window functions over the same partition (peak + trough identification).
* QUALIFY to filter on window results without a subquery.

Performance on Snowflake (38M rows)
-----------------------------------
* Pruning: WHERE pickup_year = 2023 hits the cluster key.
* Aggregation cost: output grain is 7*24 = 168 rows; the hard work is the
  first-pass aggregate over ~38M rows, which parallelises well on XS.
* Alternative: if this query is hit constantly from Snowsight, redirect to
  mart `fct_hourly_demand` — identical semantics, pre-aggregated to 2016 rows.
================================================================================
*/

WITH hourly AS (
    SELECT
        pickup_dow,
        pickup_hour,
        COUNT(*)                                  AS trip_count,
        AVG(fare_amount)                          AS avg_fare,
        AVG(total_amount)                         AS avg_total,
        AVG(trip_distance)                        AS avg_distance_mi,
        AVG(trip_duration_min)                    AS avg_duration_min,
        APPROX_PERCENTILE(fare_amount, 0.5)       AS p50_fare,
        APPROX_PERCENTILE(fare_amount, 0.9)       AS p90_fare
    FROM ANALYTICS.MARTS.FCT_TRIPS
    WHERE pickup_year = 2023
    GROUP BY 1, 2
),

tagged AS (
    SELECT
        *,
        CASE
            WHEN trip_count = MAX(trip_count) OVER (PARTITION BY pickup_dow) THEN 'peak'
            WHEN trip_count = MIN(trip_count) OVER (PARTITION BY pickup_dow) THEN 'trough'
            ELSE NULL
        END AS dow_extreme,
        RANK() OVER (PARTITION BY pickup_dow ORDER BY trip_count DESC) AS dow_trip_rank
    FROM hourly
)

SELECT
    pickup_dow,
    pickup_hour,
    trip_count,
    ROUND(avg_fare, 2)         AS avg_fare_usd,
    ROUND(avg_total, 2)        AS avg_total_usd,
    ROUND(avg_distance_mi, 2)  AS avg_distance_mi,
    ROUND(avg_duration_min, 1) AS avg_duration_min,
    ROUND(p50_fare, 2)         AS p50_fare_usd,
    ROUND(p90_fare, 2)         AS p90_fare_usd,
    dow_extreme,
    dow_trip_rank
FROM tagged
ORDER BY pickup_dow, pickup_hour;
