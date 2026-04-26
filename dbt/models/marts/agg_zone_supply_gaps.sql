{{
    config(
        materialized = 'incremental',
        unique_key   = ['pickup_date', 'pu_location_id'],
        cluster_by   = ['pickup_date', 'pu_location_id'],
        on_schema_change = 'sync_all_columns',
    )
}}

-- Business Q3: Are there zones that regularly go extended periods with no
-- pickups? What's the longest observed gap per zone per day?
--
-- Grain: one row per (pickup_date, pu_location_id).
-- Metrics:
--   • longest_gap_min   — biggest gap between consecutive pickups within the day
--   • avg_gap_min       — avg gap (gives a fuller picture than just the max)
--   • trip_count        — trips that day (context for the gaps)
--   • gaps_gt_1h / gaps_gt_3h — how many "long" gaps that day
--
-- Why incremental: this mart is the single most expensive one in the project
-- (per-zone sorted window across ~38M rows). Running it incrementally on
-- pickup_date lets backfills + daily runs cost ~O(new data), not O(full history).
-- See README brainstormer (expensive query) for the Snowflake-specific tuning
-- options — clustering on (pickup_date, pu_location_id) complements incremental.

with trips as (
    select
        pickup_date,
        pu_location_id,
        pu_borough,
        pu_zone,
        pickup_ts
    from {{ ref('int_trips_enriched') }}
    {% if is_incremental() %}
      -- Only rescan days newer than whatever we already have.
      where pickup_date >= (select coalesce(max(pickup_date), '1970-01-01'::date) from {{ this }})
    {% endif %}
),

with_gaps as (
    select
        pickup_date,
        pu_location_id,
        pu_borough,
        pu_zone,
        pickup_ts,
        datediff(
            'minute',
            lag(pickup_ts) over (
                partition by pickup_date, pu_location_id
                order by pickup_ts
            ),
            pickup_ts
        ) as gap_min
    from trips
)

select
    pickup_date,
    pu_location_id,
    any_value(pu_borough)                                         as pu_borough,
    any_value(pu_zone)                                            as pu_zone,
    count(*)                                                      as trip_count,
    max(gap_min)                                                  as longest_gap_min,
    avg(gap_min)                                                  as avg_gap_min,
    count_if(gap_min > 60)                                        as gaps_gt_1h,
    count_if(gap_min > 180)                                       as gaps_gt_3h,
    min(pickup_ts)                                                as first_pickup_ts,
    max(pickup_ts)                                                as last_pickup_ts
from with_gaps
group by 1, 2
