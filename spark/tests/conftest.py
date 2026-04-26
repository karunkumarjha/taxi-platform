"""Shared pytest fixtures for the Spark staging tests.

A single local SparkSession is created per session — Spark startup is the
slowest part of these tests (~3s), so reusing it across the suite keeps
total runtime in the seconds range.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Schema of the per-row dict the tests build into a DataFrame. Mirrors what
# read_one_month_raw returns BEFORE staging (cast + projection done; no
# derived cols, no validity rules, no trip_sk). All 19 source data columns
# are present plus source_filename + loaded_at metadata.
RAW_TRIPS_SCHEMA = StructType(
    [
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_ts", TimestampType(), True),
        StructField("dropoff_ts", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True),
        StructField("source_filename", StringType(), True),
        StructField("loaded_at", TimestampType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """One local Spark session for the whole test session."""
    spark = (
        SparkSession.builder.appName("spark_staging_tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def make_row(
    *,
    vendor_id=2,
    pickup_ts=datetime(2023, 6, 15, 12, 0, 0),
    dropoff_ts=datetime(2023, 6, 15, 12, 15, 0),
    passenger_count=1,
    trip_distance=2.5,
    ratecode_id=1,
    store_and_fwd_flag="N",
    pu_location_id=132,
    do_location_id=237,
    payment_type=1,
    fare_amount=12.0,
    extra=0.0,
    mta_tax=0.5,
    tip_amount=2.0,
    tolls_amount=0.0,
    improvement_surcharge=0.3,
    total_amount=14.5,
    congestion_surcharge=2.5,
    airport_fee=0.0,
    cbd_congestion_fee=None,  # added 2025; NULL for pre-2025 data
    source_filename="yellow_tripdata_2023-06.parquet",
    loaded_at=datetime(2023, 8, 10, 0, 0, 0),
) -> dict:
    """Factory for a "valid" trip row with reasonable defaults. Tests
    override individual fields to construct each invalid_reason scenario.
    Defaults reflect a typical Manhattan credit-card trip."""
    return {
        "vendor_id": vendor_id,
        "pickup_ts": pickup_ts,
        "dropoff_ts": dropoff_ts,
        "passenger_count": passenger_count,
        "trip_distance": trip_distance,
        "ratecode_id": ratecode_id,
        "store_and_fwd_flag": store_and_fwd_flag,
        "pu_location_id": pu_location_id,
        "do_location_id": do_location_id,
        "payment_type": payment_type,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total_amount,
        "congestion_surcharge": congestion_surcharge,
        "airport_fee": airport_fee,
        "cbd_congestion_fee": cbd_congestion_fee,
        "source_filename": source_filename,
        "loaded_at": loaded_at,
    }


@pytest.fixture(scope="session")
def trips_df_factory(spark):
    """Returns a function that turns a list of row-dicts into a DataFrame
    with the expected raw-trips schema."""

    def _make(rows: list[dict]):
        return spark.createDataFrame(rows, schema=RAW_TRIPS_SCHEMA)

    return _make


@pytest.fixture(scope="session")
def zones_df(spark):
    """A small dim_zones DataFrame covering the location_ids used in
    fixtures. Loaded from the real CSV so test data matches production."""
    repo_root = Path(__file__).resolve().parents[2]
    csv_path = repo_root / "dbt" / "seeds" / "dim_zones.csv"
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(csv_path))
        .selectExpr(
            "cast(location_id as int) as location_id",
            "borough",
            "zone_name",
            "service_zone",
        )
    )
