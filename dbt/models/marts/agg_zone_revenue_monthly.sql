{{
    config(
        materialized = 'table',
        cluster_by   = ['pickup_year', 'pickup_month'],
    )
}}

-- Business Q1: Which pickup zones generate the most revenue across 2023, and
-- does the ranking shift meaningfully by month?
--
-- Grain: one row per (pickup_year, pickup_month, pu_location_id).
-- Serves both the Snowsight dashboard (ranked bar + month slicer) and queries/.
--
-- Why include rank + year totals + month-over-month movement here (not in the
-- query): these aggregates are read repeatedly and we don't want each dashboard
-- tile re-scanning ~38M rows. Pre-computing keeps Snowsight queries cheap.

with monthly as (
    select
        pickup_year,
        pickup_month,
        pu_location_id,
        pu_borough,
        pu_zone,
        count(*)            as trip_count,
        sum(total_amount)   as gross_revenue,
        sum(fare_amount)    as fare_revenue,
        sum(tip_amount)     as tip_revenue,
        avg(total_amount)   as avg_revenue_per_trip
    from {{ ref('int_trips_enriched') }}
    group by 1, 2, 3, 4, 5
),

ranked as (
    -- First pass: per-month rank only. Snowflake doesn't allow window functions
    -- nested inside other window functions, so the LAG over this rank lives
    -- in the next CTE.
    select
        *,
        rank() over (
            partition by pickup_year, pickup_month
            order by gross_revenue desc
        ) as revenue_rank_in_month
    from monthly
),

ranked_with_prev as (
    -- Second pass: month-over-month rank movement.
    -- + = zone climbed vs prior month, - = slid down.
    select
        *,
        lag(revenue_rank_in_month) over (
            partition by pu_location_id
            order by pickup_year, pickup_month
        ) as prev_month_rank
    from ranked
),

year_totals as (
    select
        pickup_year,
        pu_location_id,
        sum(gross_revenue)                                            as yearly_gross_revenue,
        rank() over (partition by pickup_year order by sum(gross_revenue) desc) as yearly_revenue_rank
    from monthly
    group by 1, 2
)

select
    r.pickup_year,
    r.pickup_month,
    r.pu_location_id,
    r.pu_borough,
    r.pu_zone,
    r.trip_count,
    r.gross_revenue,
    r.fare_revenue,
    r.tip_revenue,
    r.avg_revenue_per_trip,
    r.revenue_rank_in_month,
    r.prev_month_rank,
    case
        when r.prev_month_rank is null then null
        else r.prev_month_rank - r.revenue_rank_in_month   -- + = climbed ranks
    end as rank_change_vs_prev_month,
    y.yearly_gross_revenue,
    y.yearly_revenue_rank
from ranked_with_prev r
left join year_totals y
  on y.pickup_year = r.pickup_year
 and y.pu_location_id = r.pu_location_id
