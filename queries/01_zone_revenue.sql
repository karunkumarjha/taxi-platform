/*
================================================================================
Business question (Q1): Which pickup zones generate the most revenue across
2023, and does the ranking shift meaningfully by month?
================================================================================

Approach
--------
Three CTEs:
  1. monthly   — revenue per (month, pickup zone)
  2. ranked    — RANK() within each month + LAG() of last month's rank per zone
  3. yearly    — zone's full-year rank, joined back on
Final output shows the zone's monthly rank, MoM movement, and year-wide rank.

SQL features used
-----------------
* Two stacked window functions (RANK OVER PARTITION BY month; LAG OVER PARTITION
  BY zone ORDER BY month) to answer "did the rank shift?" in one pass.
* A separate CTE for yearly totals joined back on zone, rather than a SUM() OVER
  — this keeps the zone-year row count low and the join cheap.
* QUALIFY on the monthly rank to keep only the top-20 per month (dashboard
  consumers rarely need the full 262-zone tail).

Performance on Snowflake (38M rows)
-----------------------------------
* Pruning: the underlying fact (int_trips_enriched) is CLUSTER BY
  (pickup_date, pu_location_id). `WHERE pickup_year = 2023` prunes to the
  relevant micro-partitions.
* Window compute: the heaviest step is the PARTITION BY month RANK() over
  ~3k zone-months — tiny, well under 1 credit on XS.
* Materialisation: if the dashboard hits this repeatedly, redirect it to
  mart `fct_zone_revenue_monthly` (already pre-computes these exact columns)
  instead of re-running the window functions.
================================================================================
*/

WITH monthly AS (
    SELECT
        pickup_year,
        pickup_month,
        pu_location_id,
        ANY_VALUE(pu_borough)   AS pu_borough,
        ANY_VALUE(pu_zone)      AS pu_zone,
        COUNT(*)                AS trip_count,
        SUM(total_amount)       AS gross_revenue
    FROM ANALYTICS.MARTS.FCT_TRIPS
    WHERE pickup_year = 2023
    GROUP BY 1, 2, 3
),

ranked AS (
    -- Rank zones within each month. Snowflake doesn't allow window functions
    -- nested inside other window functions — the LAG over this rank lives
    -- in the next CTE.
    SELECT
        *,
        RANK() OVER (
            PARTITION BY pickup_year, pickup_month
            ORDER BY gross_revenue DESC
        ) AS rank_in_month
    FROM monthly
),

ranked_with_prev AS (
    -- Month-over-month rank movement: + means the zone climbed vs prior month.
    SELECT
        *,
        LAG(rank_in_month) OVER (
            PARTITION BY pu_location_id
            ORDER BY pickup_year, pickup_month
        ) AS prev_month_rank
    FROM ranked
),

yearly AS (
    SELECT
        pu_location_id,
        SUM(gross_revenue)                                                  AS yearly_gross_revenue,
        RANK() OVER (ORDER BY SUM(gross_revenue) DESC)                      AS yearly_rank
    FROM monthly
    GROUP BY 1
)

SELECT
    r.pickup_year,
    r.pickup_month,
    r.pu_location_id,
    r.pu_borough,
    r.pu_zone,
    r.trip_count,
    ROUND(r.gross_revenue, 2)                                   AS gross_revenue_usd,
    r.rank_in_month,
    r.prev_month_rank,
    COALESCE(r.prev_month_rank - r.rank_in_month, 0)            AS rank_change_vs_prev_month,
    ROUND(y.yearly_gross_revenue, 2)                            AS yearly_gross_revenue_usd,
    y.yearly_rank
FROM ranked_with_prev r
JOIN yearly y USING (pu_location_id)
QUALIFY rank_in_month <= 20
ORDER BY pickup_month, rank_in_month;
