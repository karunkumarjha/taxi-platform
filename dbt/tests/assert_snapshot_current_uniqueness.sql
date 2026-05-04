-- ============================================================================
-- assert_snapshot_current_uniqueness — singular test
-- ============================================================================
--
-- Snapshot invariant: each `trip_bk` must have AT MOST ONE row in
-- `snp_yellow_trips` with `dbt_valid_to IS NULL` (the "current" SCD version).
--
-- Why this matters
-- ----------------
-- Every downstream layer of the platform reads
--     FROM snp_yellow_trips WHERE dbt_valid_to IS NULL
-- as its canonical source. If the snapshot has two current rows for the same
-- trip_bk, every layer below it inflates:
--   • `stg_yellow_trips` returns duplicate rows for that trip
--   • `int_trips_enriched`'s merge upserts then deletes the same trip_bk
--     in unstable order — non-deterministic Gold state
--   • aggregates double-count
--
-- The snapshot's `qualify row_number() over (partition by trip_bk ...) = 1`
-- dedup gate is *meant* to prevent this, but if the gate is ever removed or
-- the source query gains a non-deduped path, the invariant breaks silently.
-- This test is the loud alarm.
--
-- What can break the invariant
-- ----------------------------
--   • Two manual `dbt snapshot --select snp_yellow_trips` runs racing
--     (max_active_runs=1 prevents this from the DAG, but a manual operator
--     could trigger a race)
--   • The source query's `qualify` clause being removed or broken
--   • A snapshot strategy change that inserts both old + new rows as
--     "current" (e.g. setting `invalidate_hard_deletes=false` semantically wrong)
--   • Manual SQL-level UPDATE/INSERT into the snapshot table outside dbt
--
-- Returns
-- -------
-- One row per trip_bk that has > 1 current version. dbt fails the test on
-- any non-zero result. The row tells the operator exactly which trip_bk(s)
-- to investigate via:
--     SELECT * FROM snp_yellow_trips WHERE trip_bk = '<bad_bk>'
--                                      AND dbt_valid_to IS NULL;
-- ============================================================================

SELECT
    trip_bk,
    COUNT(*) AS current_version_count
FROM {{ ref('snp_yellow_trips') }}
WHERE dbt_valid_to IS NULL
GROUP BY trip_bk
HAVING COUNT(*) > 1
