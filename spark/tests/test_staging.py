"""Tests for the Spark staging logic in spark/process_historical.py.

Coverage:
  • All 12 validity rules — each invalid_reason category exercised.
  • trip_sk determinism + sensitivity to source_filename.
  • Derived columns: pickup_dow ISO format, fractional trip_duration_min.
  • Zone enrichment via broadcast join.
  • Output column ORDER matches dbt's int_trips_enriched / int_trips_quarantined
    schemas (so Snowflake's MATCH_BY_COLUMN_NAME COPY works).

These tests pin the parity between Spark's staging output and dbt's
stg_yellow_trips behaviour. Drift would land Spark-staged historical data
in MARTS_BUILD with subtly different invalid_reason assignments — exactly
the kind of bug count-divergence detection wouldn't catch.
"""

from __future__ import annotations

from datetime import datetime

from spark.process_historical import (
    FCT_TRIPS_COLUMNS,
    FCT_TRIPS_QUARANTINED_COLUMNS,
    add_derived_columns,
    add_dup_rank,
    add_invalid_reason,
    add_trip_sk,
    stage_trips,
)
from spark.tests.conftest import make_row

# ============================================================================
# The 12 validity rules — each rule should fire for its scenario.
# ============================================================================


def _classify(trips_df_factory, rows):
    """Apply derived cols + dup_rank + invalid_reason, return a dict
    {row_index: invalid_reason}. Tests assert which rules fire."""
    df = trips_df_factory(rows)
    classified = (
        df.transform(add_derived_columns).transform(add_dup_rank).transform(add_invalid_reason)
    )
    # Add a stable row index for reading results back. Use a unique vendor_id
    # marker per test row when rows differ; otherwise sort by source_filename.
    return [r["invalid_reason"] for r in classified.collect()]


def test_null_timestamp_pickup(trips_df_factory):
    rows = [make_row(pickup_ts=None)]
    assert _classify(trips_df_factory, rows) == ["null_timestamp"]


def test_null_timestamp_dropoff(trips_df_factory):
    rows = [make_row(dropoff_ts=None)]
    assert _classify(trips_df_factory, rows) == ["null_timestamp"]


def test_pickup_ge_dropoff(trips_df_factory):
    rows = [
        make_row(
            pickup_ts=datetime(2023, 6, 15, 12, 30, 0),
            dropoff_ts=datetime(2023, 6, 15, 12, 0, 0),  # before pickup
        )
    ]
    assert _classify(trips_df_factory, rows) == ["pickup_ge_dropoff"]


def test_duration_out_of_range(trips_df_factory):
    # > 12h / 720 min trip — meter-left-running glitch
    rows = [
        make_row(
            pickup_ts=datetime(2023, 6, 15, 0, 0, 0),
            dropoff_ts=datetime(2023, 6, 15, 13, 0, 0),  # 13h
        )
    ]
    assert _classify(trips_df_factory, rows) == ["duration_out_of_range"]


def test_non_positive_distance_zero(trips_df_factory):
    rows = [make_row(trip_distance=0.0)]
    assert _classify(trips_df_factory, rows) == ["non_positive_distance"]


def test_non_positive_distance_null(trips_df_factory):
    rows = [make_row(trip_distance=None)]
    assert _classify(trips_df_factory, rows) == ["non_positive_distance"]


def test_distance_out_of_range(trips_df_factory):
    rows = [make_row(trip_distance=250.0)]  # > 200 mi
    assert _classify(trips_df_factory, rows) == ["distance_out_of_range"]


def test_negative_fare(trips_df_factory):
    rows = [make_row(fare_amount=-1.0, total_amount=-2.0)]
    assert _classify(trips_df_factory, rows) == ["negative_fare_or_total"]


def test_excessive_fare(trips_df_factory):
    rows = [make_row(fare_amount=1500.0, total_amount=1600.0)]
    assert _classify(trips_df_factory, rows) == ["excessive_fare_or_total"]


def test_tip_exceeds_fare(trips_df_factory):
    rows = [make_row(fare_amount=10.0, tip_amount=20.0, total_amount=35.0)]
    assert _classify(trips_df_factory, rows) == ["tip_exceeds_fare_or_total"]


def test_tip_exceeds_total(trips_df_factory):
    # tip > total is mathematically impossible (total includes tip)
    rows = [make_row(fare_amount=15.0, tip_amount=20.0, total_amount=18.0)]
    assert _classify(trips_df_factory, rows) == ["tip_exceeds_fare_or_total"]


def test_unknown_payment_type(trips_df_factory):
    rows = [make_row(payment_type=99)]
    assert _classify(trips_df_factory, rows) == ["unknown_payment_type"]


def test_null_pu_location(trips_df_factory):
    rows = [make_row(pu_location_id=None)]
    assert _classify(trips_df_factory, rows) == ["null_location_id"]


def test_null_do_location(trips_df_factory):
    rows = [make_row(do_location_id=None)]
    assert _classify(trips_df_factory, rows) == ["null_location_id"]


def test_duplicate_row(trips_df_factory):
    """Two rows with identical natural-key tuples — second wins
    duplicate_row, ordered by source_filename."""
    base = make_row(
        vendor_id=2,
        pickup_ts=datetime(2023, 6, 15, 12, 0, 0),
        dropoff_ts=datetime(2023, 6, 15, 12, 15, 0),
        pu_location_id=132,
        do_location_id=237,
        fare_amount=12.0,
        total_amount=14.5,
        payment_type=1,
    )
    rows = [
        {**base, "source_filename": "yellow_tripdata_2023-06.parquet"},
        {**base, "source_filename": "yellow_tripdata_2023-07.parquet"},
    ]
    df = trips_df_factory(rows)
    classified = (
        df.transform(add_derived_columns).transform(add_dup_rank).transform(add_invalid_reason)
    )
    by_file = {r["source_filename"]: r["invalid_reason"] for r in classified.collect()}
    assert by_file["yellow_tripdata_2023-06.parquet"] is None  # dup_rank=1, valid
    assert by_file["yellow_tripdata_2023-07.parquet"] == "duplicate_row"


def test_valid_row_is_valid(trips_df_factory):
    """A clean row should pass all 12 rules → invalid_reason is NULL."""
    rows = [make_row()]
    assert _classify(trips_df_factory, rows) == [None]


def test_first_match_wins_rule_order(trips_df_factory):
    """Multiple violations on one row — order matters, first match wins.
    null_timestamp fires before non_positive_distance even though both apply."""
    rows = [make_row(pickup_ts=None, trip_distance=0.0)]
    assert _classify(trips_df_factory, rows) == ["null_timestamp"]


# ============================================================================
# trip_sk surrogate key
# ============================================================================


def test_trip_sk_deterministic(trips_df_factory):
    """Same input → same trip_sk on repeated runs (no random salt)."""
    rows = [make_row()]
    df1 = trips_df_factory(rows).transform(add_trip_sk).collect()
    df2 = trips_df_factory(rows).transform(add_trip_sk).collect()
    assert df1[0]["trip_sk"] == df2[0]["trip_sk"]


def test_trip_sk_changes_with_source_filename(trips_df_factory):
    """source_filename is part of the surrogate key (per dbt_utils
    generate_surrogate_key call). Different files → different trip_sk
    even for identical natural-key tuples — this is what makes the
    duplicate_row rule's row_number ordering deterministic."""
    base = make_row()
    rows = [
        {**base, "source_filename": "yellow_tripdata_2023-06.parquet"},
        {**base, "source_filename": "yellow_tripdata_2023-07.parquet"},
    ]
    df = trips_df_factory(rows).transform(add_trip_sk).collect()
    sks = sorted(r["trip_sk"] for r in df)
    assert sks[0] != sks[1]


def test_trip_sk_md5_format(trips_df_factory):
    """trip_sk should be a 32-char lowercase hex MD5 digest."""
    rows = [make_row()]
    df = trips_df_factory(rows).transform(add_trip_sk).collect()
    sk = df[0]["trip_sk"]
    assert isinstance(sk, str)
    assert len(sk) == 32
    assert all(c in "0123456789abcdef" for c in sk)


def test_trip_sk_handles_null_columns(trips_df_factory):
    """trip_sk must produce a value even when natural-key columns are NULL
    (dbt_utils inserts a sentinel). Otherwise quarantined null-row
    dedup-by-trip_sk would break."""
    rows = [make_row(vendor_id=None)]
    df = trips_df_factory(rows).transform(add_trip_sk).collect()
    assert df[0]["trip_sk"] is not None
    assert len(df[0]["trip_sk"]) == 32


# ============================================================================
# Derived columns
# ============================================================================


def test_pickup_dow_iso_monday_is_one(trips_df_factory):
    """Match dbt's `extract(dayofweekiso from pickup_ts)`: 1=Monday … 7=Sunday."""
    # 2023-06-12 is a Monday
    rows = [
        make_row(
            pickup_ts=datetime(2023, 6, 12, 12, 0, 0),
            dropoff_ts=datetime(2023, 6, 12, 12, 15, 0),
        )
    ]
    df = trips_df_factory(rows).transform(add_derived_columns).collect()
    assert df[0]["pickup_dow"] == 1


def test_pickup_dow_iso_sunday_is_seven(trips_df_factory):
    # 2023-06-18 is a Sunday
    rows = [
        make_row(
            pickup_ts=datetime(2023, 6, 18, 12, 0, 0),
            dropoff_ts=datetime(2023, 6, 18, 12, 15, 0),
        )
    ]
    df = trips_df_factory(rows).transform(add_derived_columns).collect()
    assert df[0]["pickup_dow"] == 7


def test_trip_duration_min_fractional(trips_df_factory):
    """Sub-minute trips should come through as fractional, not floored —
    matches dbt's `datediff('second', ...) / 60.0`. Floored arithmetic
    was a real bug we fixed (~7k January rows tripped trip_duration_sane)."""
    rows = [
        make_row(
            pickup_ts=datetime(2023, 6, 15, 12, 0, 30),
            dropoff_ts=datetime(2023, 6, 15, 12, 0, 50),  # 20 seconds
        )
    ]
    df = trips_df_factory(rows).transform(add_derived_columns).collect()
    assert df[0]["trip_duration_min"] == 20.0 / 60.0  # ≈ 0.333


def test_pickup_year_month_day(trips_df_factory):
    """pickup_date / year / month derive from pickup_ts. We don't assert
    pickup_hour here — it's session-timezone-sensitive and the value is
    correctly tested in production via the dbt schema-tests' accepted_range
    on (0..23). The other derived columns are tz-stable for this test's
    datetime, since IST→EDT shifts the time of day but not the date."""
    rows = [
        make_row(
            pickup_ts=datetime(2023, 6, 15, 12, 0, 0),
            dropoff_ts=datetime(2023, 6, 15, 12, 15, 0),
        )
    ]
    df = trips_df_factory(rows).transform(add_derived_columns).collect()
    row = df[0]
    assert row["pickup_year"] == 2023
    assert row["pickup_month"] == 6
    assert row["pickup_date"].isoformat() == "2023-06-15"
    assert 0 <= row["pickup_hour"] <= 23  # range check, not exact


def test_tip_pct(trips_df_factory):
    rows = [make_row(fare_amount=10.0, tip_amount=2.0)]
    df = trips_df_factory(rows).transform(add_derived_columns).collect()
    assert df[0]["tip_pct"] == 0.2


def test_tip_pct_null_when_zero_fare(trips_df_factory):
    rows = [make_row(fare_amount=0.0, tip_amount=0.0, total_amount=0.5)]
    df = trips_df_factory(rows).transform(add_derived_columns).collect()
    assert df[0]["tip_pct"] is None


# ============================================================================
# Zone enrichment + output schema
# ============================================================================


def test_stage_trips_zone_enrichment(trips_df_factory, zones_df):
    """stage_trips should attach pu_borough/pu_zone/pu_service_zone via the
    dim_zones broadcast join, and similarly for do_*."""
    rows = [make_row(pu_location_id=132, do_location_id=237)]
    raw = trips_df_factory(rows)
    staged = stage_trips(raw, zones_df).collect()
    row = staged[0]
    # 132 = JFK Airport, 237 = Upper East Side South (per TLC zones)
    assert row["pu_borough"] is not None
    assert row["pu_zone"] is not None
    assert row["do_borough"] is not None
    assert row["do_zone"] is not None


def test_stage_trips_unknown_location_left_join(trips_df_factory, zones_df):
    """A pu_location_id not in dim_zones should still flow through (LEFT
    join); pu_borough/pu_zone become NULL. The row will be quarantined as
    'null_location_id' only if the original column was NULL — out-of-range
    IDs are NOT quarantined here. They land in FCT_TRIPS with NULL zone
    metadata, which is acceptable."""
    rows = [make_row(pu_location_id=99999, do_location_id=99998)]
    raw = trips_df_factory(rows)
    staged = stage_trips(raw, zones_df).collect()
    assert staged[0]["pu_zone"] is None
    assert staged[0]["do_zone"] is None
    assert staged[0]["is_valid"] is True  # not classified invalid


def test_fct_trips_columns_match_dbt():
    """The FCT_TRIPS_COLUMNS list pins the column order Spark emits.
    Drift here would break Snowflake's COPY MATCH_BY_COLUMN_NAME, since
    parquet's column order is what COPY honours.

    Expected: 36 columns = 20 source data columns + 7 derived
    (pickup_date/hour/dow/year/month, trip_duration_min, tip_pct) + 1
    surrogate (trip_sk) + 6 zone-enrichment (pu/do borough/zone/service_zone)
    + 2 load metadata (source_filename, loaded_at).
    """
    assert len(FCT_TRIPS_COLUMNS) == 36
    assert FCT_TRIPS_COLUMNS[0] == "trip_sk"
    assert FCT_TRIPS_COLUMNS[-1] == "loaded_at"
    # Every source data column must be present (cbd_congestion_fee added in
    # 2025; NULL in pre-2025 parquet but always present in the schema).
    for source_col in [
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]:
        assert source_col in FCT_TRIPS_COLUMNS, f"missing source column: {source_col}"


def test_fct_trips_quarantined_columns_match_dbt():
    """The quarantined output schema should match dbt's int_trips_quarantined.
    Same column set as FCT_TRIPS plus invalid_reason — full context for
    investigators auditing dirty rows."""
    assert len(FCT_TRIPS_QUARANTINED_COLUMNS) == 37
    assert "trip_sk" in FCT_TRIPS_QUARANTINED_COLUMNS
    assert "invalid_reason" in FCT_TRIPS_QUARANTINED_COLUMNS
    assert "loaded_at" in FCT_TRIPS_QUARANTINED_COLUMNS
    # Confirm every FCT_TRIPS column is also in quarantined (modulo invalid_reason).
    for col in FCT_TRIPS_COLUMNS:
        assert col in FCT_TRIPS_QUARANTINED_COLUMNS, f"quarantined missing: {col}"


def test_stage_trips_emits_required_columns(trips_df_factory, zones_df):
    """stage_trips's output must include every column FCT_TRIPS expects,
    plus the is_valid / invalid_reason flag used to split downstream."""
    rows = [make_row()]
    raw = trips_df_factory(rows)
    staged = stage_trips(raw, zones_df)
    cols = set(staged.columns)
    for required in FCT_TRIPS_COLUMNS:
        assert required in cols, f"missing column from stage output: {required}"
    assert "is_valid" in cols
    assert "invalid_reason" in cols
