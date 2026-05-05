/*
================================================================================
Business question (Q1): Which pickup zones generate the most revenue across
2023, and does the ranking shift meaningfully by month?
================================================================================

Approach
--------
Read both grains of the revenue mart:
  • agg_zone_revenue_monthly  — per-month rank already computed
                                (`revenue_rank_in_month`)
  • agg_zone_revenue_yearly   — per-year rank already computed
                                (`yearly_revenue_rank`, `yearly_gross_revenue`)

Join them on (pickup_year, pu_location_id) so each output row carries both
the zone's monthly rank and its full-year rank. A LAG window over the
monthly rank computes month-over-month rank movement per zone — that's
the only piece of windowing the marts don't already pre-compute, since
MoM rank deltas span months and would force a year-grain rebuild.

SQL features used
-----------------
* LAG window over (pu_location_id, year, month) for MoM rank shift —
  the metric the business question explicitly asks for ("does the
  ranking shift meaningfully by month?").
* QUALIFY on the pre-computed monthly rank — keeps the top-20 zones
  per month without a subquery.

Performance on Snowflake
------------------------
* Reads marts, not FCT_TRIPS — ~3k rows for monthly (12 × 263 zones) and
  ~263 rows for yearly. Sub-second on WH_XS regardless of warehouse load.
* Marts are clustered on `pickup_year, pickup_month` (monthly) and
  `pickup_year` (yearly) — `WHERE pickup_year = 2023` prunes cleanly.
* The MoM LAG is the only computation here; everything else is a
  metadata-light join + filter. Result-cache hits the second query of
  the same year.
================================================================================
*/

SELECT
    m.pickup_year,
    m.pickup_month,
    m.pu_location_id,
    m.pu_borough,
    m.pu_zone,
    m.trip_count,
    ROUND(m.gross_revenue, 2)                                   AS gross_revenue_usd,
    m.revenue_rank_in_month,
    LAG(m.revenue_rank_in_month) OVER (
        PARTITION BY m.pu_location_id
        ORDER BY m.pickup_year, m.pickup_month
    )                                                           AS prev_month_rank,
    COALESCE(
        LAG(m.revenue_rank_in_month) OVER (
            PARTITION BY m.pu_location_id
            ORDER BY m.pickup_year, m.pickup_month
        ) - m.revenue_rank_in_month,
        0
    )                                                           AS rank_change_vs_prev_month,
    ROUND(y.yearly_gross_revenue, 2)                            AS yearly_gross_revenue_usd,
    y.yearly_revenue_rank
FROM ANALYTICS.MARTS.AGG_ZONE_REVENUE_MONTHLY m
JOIN ANALYTICS.MARTS.AGG_ZONE_REVENUE_YEARLY  y
    ON  y.pickup_year    = m.pickup_year
    AND y.pu_location_id = m.pu_location_id
WHERE m.pickup_year = 2023
QUALIFY m.revenue_rank_in_month <= 20
ORDER BY m.pickup_month, m.revenue_rank_in_month;
