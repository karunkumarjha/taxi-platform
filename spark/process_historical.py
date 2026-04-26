"""
Process ONE month of NYC TLC Yellow Taxi parquet via PySpark and emit a daily
per-zone aggregate to S3, partitioned by year/month for downstream querying.

Why month-by-month (not whole-year batch):
    The TLC dataset goes back to 2009 — ~1.5B rows across 14 years. A monolithic
    job is fragile (any failure costs everything). Month-grain gives:
      * Per-month idempotency — re-run a bad month without touching siblings.
      * Failure isolation — June crashing doesn't take July with it.
      * Easy backfill — submit one job per month, sequential or parallel.
      * Cost control — stop anytime; at worst lose one month's compute.
    Each invocation reads ONE TLC parquet file, writes ONE partition.

Why dynamic partition overwrite:
    Output is partitioned by (year, month). Default Spark overwrite mode wipes
    the entire output directory — a single-month re-run would delete other
    months. With `partitionOverwriteMode = dynamic` the overwrite only touches
    the partition(s) the job actually wrote to.

Optimisations (called out throughout):
    1. Projection pushdown   — read only the 9 columns we aggregate.
    2. Predicate pushdown    — year/month filter applied before any shuffle.
    3. Broadcast join        — zone lookup (~265 rows) is tiny.
    4. AQE                   — adaptive skew handling for hot zones (JFK/LGA).
    5. Snappy parquet output — EMR default; explicit for cross-platform repro.
    6. maxRecordsPerFile     — caps per-file row count, ~128 MB target.

Run locally:
    spark-submit spark/process_historical.py \\
        --input  s3://analytics-data-xxxx/raw/ \\
        --output s3://analytics-data-xxxx/analytics/daily_zone_aggregates/ \\
        --zones  s3://analytics-data-xxxx/spark-scripts/dim_zones.csv \\
        --year   2023 \\
        --month  1

Run on EMR Serverless:
    Use spark/submit_emr.py — it loops over months and submits one job each.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

log = logging.getLogger("process_historical")


# Fixed schema for the zone dim. Avoids inference cost on every job.
ZONE_SCHEMA = StructType([
    StructField("location_id", IntegerType(), nullable=False),
    StructField("borough", StringType(), nullable=True),
    StructField("zone_name", StringType(), nullable=True),
    StructField("service_zone", StringType(), nullable=True),
])


def build_session(app_name: str) -> SparkSession:
    """Configure the Spark session with all the knobs this workload needs."""
    return (
        SparkSession.builder
        .appName(app_name)
        # Adaptive Query Execution — handles skew on hot zones (JFK/LGA) by
        # splitting their shuffle partitions automatically.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Dynamic partition overwrite — single-month re-runs only touch their
        # own partition, not the whole output dir. CRUCIAL for month-by-month.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Parquet writer
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxRecordsPerFile", 2_000_000)
        # Preserve naive timestamps from TLC parquet (no TZ shift).
        .config("spark.sql.parquet.int96AsTimestamp", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def read_one_month(spark: SparkSession, input_uri: str, year: int, month: int):
    """Read a single TLC month's parquet with projection + predicate pushdown."""
    if not input_uri.endswith("/"):
        input_uri += "/"

    path = f"{input_uri}yellow_tripdata_{year:04d}-{month:02d}.parquet"
    log.info("reading %s", path)

    return (
        spark.read
        .parquet(path)
        .select(
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("PULocationID").cast("int").alias("pu_location_id"),
            F.col("DOLocationID").cast("int").alias("do_location_id"),
            F.col("payment_type").cast("int").alias("payment_type"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("total_amount").cast("double").alias("total_amount"),
        )
        # Defensive: TLC files have a small cross-month tail (e.g. some Feb
        # rows in the Jan file). Filter to the requested month so each
        # output partition is exactly the month asked for.
        .where((F.year("pickup_ts") == year) & (F.month("pickup_ts") == month))
    )


def read_zones(spark: SparkSession, zones_uri: str):
    """Tiny dim, ~265 rows. Read with explicit schema; broadcast in the join."""
    return (
        spark.read
        .option("header", True)
        .schema(ZONE_SCHEMA)
        .csv(zones_uri)
    )


def aggregate(trips, zones):
    """Validity-filter + zone-join + roll up to (zone, pickup_date)."""
    valid = trips.where(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("pickup_ts") < F.col("dropoff_ts"))
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 200)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & F.col("pu_location_id").isNotNull()
    )

    enriched = valid.join(
        F.broadcast(zones),     # ~265-row dim — broadcast always wins
        valid["pu_location_id"] == zones["location_id"],
        how="left",
    ).select(
        F.to_date("pickup_ts").alias("pickup_date"),
        F.year("pickup_ts").alias("year"),
        F.month("pickup_ts").alias("month"),
        F.col("pu_location_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone_name").alias("pu_zone"),
        F.col("payment_type"),
        F.col("trip_distance"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
    )

    return enriched.groupBy("year", "month", "pickup_date", "pu_location_id").agg(
        F.first("pu_borough", ignorenulls=True).alias("pu_borough"),
        F.first("pu_zone",    ignorenulls=True).alias("pu_zone"),
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("total_amount").alias("gross_revenue"),
        F.sum("fare_amount").alias("fare_revenue"),
        F.sum("tip_amount").alias("tip_revenue"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance_mi"),
        F.expr("approx_percentile(fare_amount, 0.5)").alias("p50_fare"),
        F.expr("approx_percentile(fare_amount, 0.9)").alias("p90_fare"),
        F.count(F.when(F.col("payment_type") == 1, 1)).alias("credit_trips"),
        F.count(F.when(F.col("payment_type") == 2, 1)).alias("cash_trips"),
    )


def write_one_month(df, output_uri: str) -> None:
    """Write the single-month aggregate. Dynamic partition overwrite means
    only the (year, month) partition we wrote is replaced — siblings untouched."""
    log.info("writing → %s (partitionBy=year,month, mode=overwrite/dynamic)", output_uri)
    (
        df.repartition("year", "month")     # one task per output partition
        .write
        .mode("overwrite")                   # combined with dynamic mode = partition-only overwrite
        .partitionBy("year", "month")
        .parquet(output_uri)
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entry: read one TLC month, aggregate by (zone, day), write a partition."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",  required=True, help="s3://.../raw/")
    parser.add_argument("--output", required=True, help="s3://.../analytics/daily_zone_aggregates/")
    parser.add_argument("--zones",  required=True, help="s3://.../spark-scripts/dim_zones.csv")
    parser.add_argument("--year",   required=True, type=int)
    parser.add_argument("--month",  required=True, type=int, choices=range(1, 13),
                        metavar="{1..12}")
    parser.add_argument("--app-name", default=None,
                        help="Spark app name. Defaults to taxi_monthly_<year>_<month>.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app_name = args.app_name or f"taxi_monthly_{args.year}_{args.month:02d}"
    log.info("year=%d month=%d input=%s output=%s",
             args.year, args.month, args.input, args.output)

    spark = build_session(app_name)
    try:
        trips = read_one_month(spark, args.input, args.year, args.month)
        zones = read_zones(spark, args.zones)
        aggregates = aggregate(trips, zones)
        write_one_month(aggregates, args.output)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
