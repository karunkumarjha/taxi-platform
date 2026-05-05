/*
================================================================================
Business question (Q4): What's the relationship between trip distance, payment
type, and tip percentage? Are there zones where passengers tip significantly
more or less?
================================================================================

Approach
--------
Read `agg_zone_tip_behaviour_yearly` — the year-grain rollup over the
monthly tip-behaviour mart. The yearly mart pre-computes trip-weighted
average tip percentage per (pickup_year, pu_location_id, distance_bucket,
payment_type), filtered to credit-card trips here because cash tips are
not recorded by the meter (called out in the README data-quality section).

A window function compares each zone's average tip percentage to the
city-wide baseline for the same distance bucket — the "above / below
average" signal the business question asks for.

Note on percentiles
-------------------
The yearly mart deliberately omits p50 / p90 of tip_pct because
percentiles are not summable across months — they can't be derived
from monthly aggregates. The monthly mart (`agg_zone_tip_behaviour_monthly`)
holds month-grain percentiles if needed; for a "single year-month
percentile" query, switch the source to that mart and add a month
predicate.

SQL features used
-----------------
* Window function over `distance_bucket` to compute the city-wide
  average tip percentage as a comparison baseline.
* Predicate `payment_type = 1` to scope to credit-card trips (the
  only ones with reliable tip data).
* Sample-size floor on `trip_count` to drop low-volume zones from the
  output and avoid noise.

Performance on Snowflake
------------------------
* Reads the yearly mart — ~265 zones × 6 buckets × payment types = a few
  thousand rows. Sub-second on WH_XS.
* Cluster key is `pickup_year, pu_location_id`; the year predicate
  prunes to the relevant micro-partitions.
* Result cache catches repeat queries within 24h.
================================================================================
*/

SELECT
    pu_location_id,
    pu_borough,
    pu_zone,
    distance_bucket,
    trip_count,
    ROUND(avg_tip_pct_credit * 100, 2)                                      AS avg_tip_pct,
    ROUND(AVG(avg_tip_pct_credit) OVER (PARTITION BY distance_bucket) * 100, 2)
                                                                            AS city_avg_tip_pct,
    ROUND((avg_tip_pct_credit
           - AVG(avg_tip_pct_credit) OVER (PARTITION BY distance_bucket)
          ) * 100, 2)                                                       AS tip_pct_deviation_pp,
    ROUND(avg_tip, 2)                                                       AS avg_tip_usd
FROM ANALYTICS.MARTS.AGG_ZONE_TIP_BEHAVIOUR_YEARLY
WHERE pickup_year   = 2023
  AND payment_type  = 1            -- credit card; cash tips are not recorded
  AND trip_count   >= 1000         -- statistically meaningful sample
ORDER BY distance_bucket, tip_pct_deviation_pp DESC;
