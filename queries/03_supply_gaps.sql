/*
================================================================================
Business question (Q3): Are there zones that regularly go extended periods with
no pickups? What's the longest observed gap per zone per day?
================================================================================

Approach
--------
Two views into the same mart, surfaced together:

  1. Per-zone "underserved-ness" leaderboard — across the whole year, which
     zones most often see long gaps between pickups?  Ranks by both *average*
     longest-gap-of-day and the *count of days* a zone hit > 3h between
     pickups.  Top entries here are the zones a dispatcher should prioritise.

  2. Per-zone single worst day — the day that zone went the longest without a
     pickup, plus how many gaps over 1h and 3h that day saw.  Useful for
     spotting one-off anomalies vs structural under-supply.

The mart `AGG_ZONE_SUPPLY_GAPS` already pre-computes these per (pickup_date,
pu_location_id), so this query is a pure rollup.

SQL features used
-----------------
* CTE per view (`per_zone_summary`, `worst_day_per_zone`) so the final SELECT
  joins them — single round-trip, no repeated table scan.
* `QUALIFY` on a `ROW_NUMBER()` window to pick the worst day per zone in one
  pass (Snowflake-native, avoids a self-join).
* `COUNT_IF` for thresholded gap counts — clearer than `SUM(CASE WHEN …)`.

Performance on Snowflake (38M underlying rows, 2023)
----------------------------------------------------
* Hits the mart, not the fact — ~265 zones × ~365 days = ~96k rows scanned,
  not millions.  Sub-second on WH_XS.
* Mart is clustered on `(pickup_date, pu_location_id)` — pruning kicks in if
  you scope to a date range with `WHERE pickup_date BETWEEN ...`.
* The dashboard reading this can hit Snowflake's result cache (24h TTL) when
  re-querying the same year.
================================================================================
*/

WITH per_zone_summary AS (
    SELECT
        pu_location_id,
        ANY_VALUE(pu_borough)             AS pu_borough,
        ANY_VALUE(pu_zone)                AS pu_zone,
        COUNT(*)                          AS days_with_pickups,
        SUM(trip_count)                   AS total_trips,
        AVG(longest_gap_min)              AS avg_longest_gap_min,
        MAX(longest_gap_min)              AS max_longest_gap_min,
        AVG(avg_gap_min)                  AS avg_within_day_gap_min,
        SUM(gaps_gt_1h)                   AS total_gaps_gt_1h,
        SUM(gaps_gt_3h)                   AS total_gaps_gt_3h,
        COUNT_IF(longest_gap_min > 180)   AS days_with_gap_gt_3h,
        COUNT_IF(longest_gap_min > 60)    AS days_with_gap_gt_1h
    FROM ANALYTICS.MARTS.AGG_ZONE_SUPPLY_GAPS
    WHERE pickup_date BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY pu_location_id
),

worst_day_per_zone AS (
    -- The single worst day per zone — longest_gap_min is the metric of record.
    -- ROW_NUMBER + QUALIFY keeps just the top-1 row per zone in one pass.
    SELECT
        pu_location_id,
        pickup_date              AS worst_day,
        longest_gap_min          AS worst_day_longest_gap_min,
        gaps_gt_1h               AS worst_day_gaps_gt_1h,
        gaps_gt_3h               AS worst_day_gaps_gt_3h,
        trip_count               AS worst_day_trip_count
    FROM ANALYTICS.MARTS.AGG_ZONE_SUPPLY_GAPS
    WHERE pickup_date BETWEEN '2023-01-01' AND '2023-12-31'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pu_location_id
        ORDER BY longest_gap_min DESC, pickup_date
    ) = 1
)

SELECT
    s.pu_borough,
    s.pu_zone,
    s.pu_location_id,
    s.days_with_pickups,
    s.total_trips,
    -- Underserved-ness signals
    ROUND(s.avg_longest_gap_min,    1) AS avg_daily_longest_gap_min,
    ROUND(s.avg_within_day_gap_min, 1) AS avg_within_day_gap_min,
    s.total_gaps_gt_1h,
    s.total_gaps_gt_3h,
    s.days_with_gap_gt_1h,
    s.days_with_gap_gt_3h,
    -- Worst observed day for this zone
    w.worst_day,
    w.worst_day_longest_gap_min,
    w.worst_day_trip_count,
    -- Rank zones by structural under-supply: more days with > 3h gaps = worse
    RANK() OVER (
        ORDER BY s.days_with_gap_gt_3h DESC, s.avg_longest_gap_min DESC
    ) AS underserved_rank
FROM per_zone_summary s
LEFT JOIN worst_day_per_zone w USING (pu_location_id)
-- Filter out zones with negligible activity to avoid noise from
-- unmetered / industrial zones that just don't see taxis.
WHERE s.days_with_pickups >= 30      -- at least one pickup on a month's worth of days
  AND s.total_trips      >= 1000     -- at least 1k trips for the whole year
ORDER BY underserved_rank
LIMIT 50;
