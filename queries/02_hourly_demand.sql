/*
================================================================================
Business question (Q2): How does trip volume and average fare vary across
hours of the day? When are the peak and trough periods?
================================================================================

Approach
--------
Read agg_hourly_demand_yearly — the year-grain rollup over the monthly
hourly_demand mart. It already holds `trip_count` and trip-weighted
`avg_fare` per (pickup_year, pickup_dow, pickup_hour), so the business
question is answered by:

  1. SELECT the year of interest                        — pruning
  2. tag the peak / trough hour per day-of-week         — window functions
  3. order by (dow, hour) for a readable matrix         — straightforward

The yearly mart smooths month-to-month variation so the "peak" and
"trough" labels reflect the year as a whole rather than any single month.

SQL features used
-----------------
* Two window functions over the same partition: MAX/MIN over
  `pickup_dow` to identify the busiest and quietest hour per DOW, and
  RANK over the same partition for the full ordering.

Performance on Snowflake
------------------------
* Reads the yearly mart — 7 × 24 = 168 rows. Trivially cheap.
* Cluster key is `pickup_year`; the predicate prunes to one year's
  partition. Result cache catches repeat dashboard hits within the
  24h cache TTL.
* If month-level granularity is later wanted, switch the source to
  `agg_hourly_demand_monthly` and group by (pickup_year, pickup_month, dow,
  hour) — same shape, one more grouping column.
================================================================================
*/

WITH tagged AS (
    SELECT
        pickup_dow,
        pickup_hour,
        trip_count,
        avg_fare,
        avg_total,
        avg_distance,
        avg_duration_min,
        CASE
            WHEN trip_count = MAX(trip_count) OVER (PARTITION BY pickup_dow) THEN 'peak'
            WHEN trip_count = MIN(trip_count) OVER (PARTITION BY pickup_dow) THEN 'trough'
            ELSE NULL
        END                                                         AS dow_extreme,
        RANK() OVER (PARTITION BY pickup_dow ORDER BY trip_count DESC) AS dow_trip_rank
    FROM ANALYTICS.MARTS.AGG_HOURLY_DEMAND_YEARLY
    WHERE pickup_year = 2023
)

SELECT
    pickup_dow,
    pickup_hour,
    trip_count,
    ROUND(avg_fare, 2)         AS avg_fare_usd,
    ROUND(avg_total, 2)        AS avg_total_usd,
    ROUND(avg_distance, 2)     AS avg_distance_mi,
    ROUND(avg_duration_min, 1) AS avg_duration_min,
    dow_extreme,
    dow_trip_rank
FROM tagged
ORDER BY pickup_dow, pickup_hour;
