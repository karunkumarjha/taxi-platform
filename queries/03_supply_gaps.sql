/*
================================================================================
Business question (Q3): Are there zones that regularly go extended periods
with no pickups? What's the longest observed gap per zone per day?
================================================================================

Approach
--------
Two views into the supply-gap mart, surfaced together:

  1. Per-zone "underserved-ness" leaderboard — across the whole year,
     which zones most often see long gaps between pickups?  Reads the
     yearly mart directly (`agg_zone_supply_gaps_yearly`) which already
     pre-computes per-(year, zone): `longest_gap_min_year`,
     `avg_longest_gap_per_day`, `days_with_gap_over_60min`,
     `days_with_gap_over_3h`, plus supporting context.

  2. Per-zone single worst day — the specific day that zone went the
     longest without a pickup, plus the gap-count buckets that day.
     Pulled from the daily mart (`agg_zone_supply_gaps_daily`) since the
     yearly rollup doesn't keep the date of the worst day.

Both joined on (pickup_year, pu_location_id) and ranked by structural
under-supply — zones with the most days exceeding the 3h gap threshold
land at the top.

SQL features used
-----------------
* QUALIFY on a ROW_NUMBER() window to pick the worst day per zone in one
  pass (Snowflake-native, avoids a self-join).
* RANK over the joined output to surface the most-underserved zones first.

Performance on Snowflake
------------------------
* Yearly mart hit: ~265 rows for 2023 (one row per zone). Negligible.
* Daily mart hit for the worst-day pull: ~265 zones × ~365 days ≈ 96k
  rows scanned, pruned via the date-range predicate; ROW_NUMBER picks
  the top-1 per zone in a single pass. Daily mart is clustered on
  (pickup_date, pu_location_id), so the filter prunes micro-partitions.
* Result-cache friendly — repeat dashboard hits within 24h hit Snowflake's
  cache as long as the underlying marts haven't been rebuilt.
================================================================================
*/

WITH worst_day_per_zone AS (
    -- Single worst day per zone — `longest_gap_min` is the metric of record.
    -- ROW_NUMBER + QUALIFY keeps just the top-1 row per zone in one pass.
    SELECT
        pu_location_id,
        pickup_date              AS worst_day,
        longest_gap_min          AS worst_day_longest_gap_min,
        gaps_gt_1h               AS worst_day_gaps_gt_1h,
        gaps_gt_3h               AS worst_day_gaps_gt_3h,
        trip_count               AS worst_day_trip_count
    FROM ANALYTICS.MARTS.AGG_ZONE_SUPPLY_GAPS_DAILY
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
    s.days_with_trips,
    s.total_trips,
    -- Underserved-ness signals (year-aggregated, mart-precomputed)
    ROUND(s.avg_longest_gap_per_day, 1)     AS avg_daily_longest_gap_min,
    s.longest_gap_min_year                  AS worst_observed_gap_min,
    s.days_with_gap_over_60min,
    s.days_with_gap_over_3h,
    -- Worst observed day for this zone (joined from daily mart)
    w.worst_day,
    w.worst_day_longest_gap_min,
    w.worst_day_trip_count,
    -- Rank zones by structural under-supply: more days with > 3h gaps = worse
    RANK() OVER (
        ORDER BY s.days_with_gap_over_3h DESC, s.avg_longest_gap_per_day DESC
    )                                        AS underserved_rank
FROM ANALYTICS.MARTS.AGG_ZONE_SUPPLY_GAPS_YEARLY s
LEFT JOIN worst_day_per_zone w
    ON w.pu_location_id = s.pu_location_id
WHERE s.pickup_year = 2023
  -- Filter out zones with negligible activity to avoid noise from
  -- unmetered / industrial zones that just don't see taxis.
  AND s.days_with_trips >= 30      -- at least one pickup on a month's worth of days
  AND s.total_trips     >= 1000    -- at least 1k trips for the whole year
ORDER BY underserved_rank
LIMIT 50;
