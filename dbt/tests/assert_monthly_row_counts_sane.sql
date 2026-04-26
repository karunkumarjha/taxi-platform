-- Singular test: flags months whose trip count differs from the prior month by
-- more than var('monthly_rowcount_tolerance') (default 40%).
--
-- Catches two real failure modes this dataset has:
--   1. Partial ingest — only N of the 12 files made it to S3, COPY loaded a
--      sliver of a month, nothing downstream noticed.
--   2. TLC re-publishing a month with corrected data that drops/adds rows
--      silently.
--
-- Only compares months that BOTH have substantial volume (>=
-- var('monthly_rowcount_min', default 10000)). This avoids false positives
-- from the cross-month bleed: when only January is loaded, the file's
-- handful of February-tail rows would otherwise look like a 99.99% drop.
--
-- First calendar month has no prior to compare against → excluded.

with monthly as (
    select
        pickup_year,
        pickup_month,
        count(*) as trip_count
    from {{ ref('int_trips_enriched') }}
    group by 1, 2
),

with_prev as (
    select
        pickup_year,
        pickup_month,
        trip_count,
        lag(trip_count) over (order by pickup_year, pickup_month) as prev_trip_count
    from monthly
),

anomalies as (
    select
        pickup_year,
        pickup_month,
        trip_count,
        prev_trip_count,
        abs(trip_count - prev_trip_count) / nullif(prev_trip_count, 0) as pct_change
    from with_prev
    where prev_trip_count is not null
      and trip_count      >= {{ var('monthly_rowcount_min', 10000) }}
      and prev_trip_count >= {{ var('monthly_rowcount_min', 10000) }}
)

select *
from anomalies
where pct_change > {{ var('monthly_rowcount_tolerance') }}
