{{
    config(
        materialized        = 'incremental',
        unique_key          = ['pickup_year', 'pickup_month', 'pu_location_id'],
        incremental_strategy= 'delete+insert',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_year', 'pickup_month'],
    )
}}

-- Business Q1: Which pickup zones generate the most revenue across the year?
--
-- Grain: one row per (pickup_year, pickup_month, pu_location_id).
-- Serves the dashboard (ranked bar + month slicer) and queries/01_zone_revenue.sql.
--
-- Year-grain self-healing incremental: yearly_gross_revenue and
-- yearly_revenue_rank depend on cross-month state, so when ANY month within
-- a year changes, the WHOLE year is rebuilt from FCT_TRIPS. Detection runs
-- at the year level (sum of row counts per year compared between fct and
-- this mart) — a divergent year triggers a rebuild for all of its months.
--
-- Cost tradeoff (deliberate): full-year rebuild is O(year-rows) — for 2023
-- that's ~38M rows scanned per divergent run, even when only ~3M (one month's
-- additions) actually changed. We accept the wasted scan in exchange for
-- correctness simplicity: yearly_* fields stay consistent without bookkeeping
-- a per-row "year_total dirty" flag. At Snowflake WH_XS this rebuild takes
-- ~30-60s, dominated by the COUNT/SUM aggregations Snowflake handles in
-- columnstore. At 1.5B-row scale this would be the place to revisit (e.g.,
-- streams + tasks for true row-level incremental).

with fct_year_counts as (
    select pickup_year, count(*) as fct_cnt
    from {{ ref('int_trips_enriched') }}
    group by 1
),

years_to_rebuild as (
    select fct.pickup_year
    from fct_year_counts fct
    {% if is_incremental() %}
    left join (
        select pickup_year, sum(trip_count) as agg_cnt
        from {{ this }}
        group by 1
    ) agg using (pickup_year)
    where
        (agg.agg_cnt is not null and agg.agg_cnt != fct.fct_cnt)
        or
        (agg.agg_cnt is null and fct.fct_cnt >= {{ var('phantom_month_threshold') }})
    {% else %}
    where fct.fct_cnt >= {{ var('phantom_month_threshold') }}
    {% endif %}
),

monthly as (
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
    where pickup_year in (select pickup_year from years_to_rebuild)
    group by 1, 2, 3, 4, 5
),

ranked as (
    select
        *,
        rank() over (
            partition by pickup_year, pickup_month
            order by gross_revenue desc
        ) as revenue_rank_in_month
    from monthly
),

year_totals as (
    select
        pickup_year,
        pu_location_id,
        sum(gross_revenue)                                                       as yearly_gross_revenue,
        rank() over (partition by pickup_year order by sum(gross_revenue) desc)  as yearly_revenue_rank
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
    y.yearly_gross_revenue,
    y.yearly_revenue_rank
from ranked r
left join year_totals y
  on y.pickup_year = r.pickup_year
 and y.pu_location_id = r.pu_location_id
