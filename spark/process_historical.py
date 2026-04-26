"""
Process ONE month of NYC TLC Yellow Taxi parquet via PySpark, replicate dbt's
staging logic (12 validity rules + trip_sk + derived columns + zone enrichment),
and write split FCT_TRIPS / FCT_TRIPS_QUARANTINED parquet to S3 — ready for
the dbt_pipeline DAG to pick up via COPY INTO MARTS_BUILD.

Pipeline role:
    Spark = historical bulk staging. Reads raw monthly TLC parquet from S3,
    produces fact-grain output that mirrors dbt's int_trips_enriched and
    int_trips_quarantined schemas, writes to s3://.../staged-marts/. The
    dbt_pipeline DAG's load_spark_staged_into_marts_build macro then COPY-INTOs
    these files into MARTS_BUILD during the next dbt run.

    Live monthly path is dbt-only — Spark's role is to handle the historical
    backfill where running dbt on billions of rows would be expensive.

Why month-by-month:
    The TLC dataset goes back to 2009 (~1.5B rows across 14 years). A monolithic
    job is fragile (any failure costs everything). Month-grain gives:
      * Per-month idempotency — re-run a bad month without touching siblings.
      * Failure isolation — June crashing doesn't take July with it.
      * Easy backfill — submit one job per month, sequential or parallel.
      * Cost control — stop anytime; at worst lose one month's compute.

Output schemas:
    staged-marts/fct_trips/{tag}.parquet              -> mirrors dbt's int_trips_enriched
    staged-marts/fct_trips_quarantined/{tag}.parquet  -> mirrors dbt's int_trips_quarantined

Schema parity:
    Column NAMES match dbt's models (COPY uses MATCH_BY_COLUMN_NAME). Column
    TYPES are written via Spark's parquet — Snowflake's COPY does the implicit
    cast on the way in. trip_sk is computed in Spark; values won't byte-match
    dbt's (different cast formatting), but since Spark owns historical months
    and dbt owns live months, there's no key overlap by design.

Run locally:
    spark-submit spark/process_historical.py \\
        --input  s3://analytics-data-xxxx/raw/ \\
        --output s3://analytics-data-xxxx/staged-marts/ \\
        --zones  s3://analytics-data-xxxx/spark-scripts/dim_zones.csv \\
        --year   2010 \\
        --month  6

Run on EMR Serverless:
    Use spark/submit_emr.py — submits one job per (year, month).
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
ZONE_SCHEMA = StructType(
    [
        StructField("location_id", IntegerType(), nullable=False),
        StructField("borough", StringType(), nullable=True),
        StructField("zone_name", StringType(), nullable=True),
        StructField("service_zone", StringType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def build_session(app_name: str) -> SparkSession:
    """Configure the Spark session with all the knobs this workload needs."""
    return (
        SparkSession.builder.appName(app_name)
        # AQE handles skew on hot zones (JFK/LGA).
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Parquet writer
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxRecordsPerFile", 2_000_000)
        # Don't write Hadoop _SUCCESS marker files. They're 0-byte sentinels
        # the FileOutputCommitter creates after each job; harmless on HDFS but
        # they break Snowflake's COPY INTO when it scans the prefix and tries
        # to parse them as parquet ("file size is 0 bytes" abort). The dbt
        # macro also filters with PATTERN='.*\.parquet' as a defence-in-depth
        # belt; this config is the suspenders.
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        # Preserve naive timestamps from TLC parquet (TLC times are local NY).
        .config("spark.sql.parquet.int96AsTimestamp", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def read_one_month_raw(spark: SparkSession, input_uri: str, year: int, month: int):
    """Read one TLC month's parquet from raw/ with projection pushdown.
    Selects ALL 20 source data columns — Spark's output mirrors dbt's
    FCT_TRIPS schema exactly, so the COPY into MARTS_BUILD.FCT_TRIPS
    via MATCH_BY_COLUMN_NAME finds every column.

    `cbd_congestion_fee` was added to TLC's schema in 2025 for the NYC
    congestion-pricing-zone surcharge. Older parquet files don't have it.
    We probe the file's column list and substitute a typed NULL when
    missing — the column is always present in the output DataFrame, just
    NULL for pre-2025 data. Same approach Snowflake's COPY uses for the
    dbt-managed table.
    """
    if not input_uri.endswith("/"):
        input_uri += "/"

    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    path = f"{input_uri}{filename}"
    log.info("reading %s", path)

    raw = spark.read.parquet(path)
    available = {c.lower() for c in raw.columns}

    def col_or_null(name: str, target_type: str):
        """Return the casted source column if present (case-insensitive),
        else a typed NULL literal. Lets us include columns that didn't
        exist in older TLC schemas without per-year branching."""
        for actual in raw.columns:
            if actual.lower() == name.lower():
                return F.col(actual).cast(target_type)
        return F.lit(None).cast(target_type)

    df = raw.select(
        col_or_null("VendorID", "int").alias("vendor_id"),
        col_or_null("tpep_pickup_datetime", "timestamp").alias("pickup_ts"),
        col_or_null("tpep_dropoff_datetime", "timestamp").alias("dropoff_ts"),
        col_or_null("passenger_count", "int").alias("passenger_count"),
        col_or_null("trip_distance", "double").alias("trip_distance"),
        col_or_null("RatecodeID", "int").alias("ratecode_id"),
        col_or_null("store_and_fwd_flag", "string").alias("store_and_fwd_flag"),
        col_or_null("PULocationID", "int").alias("pu_location_id"),
        col_or_null("DOLocationID", "int").alias("do_location_id"),
        col_or_null("payment_type", "int").alias("payment_type"),
        col_or_null("fare_amount", "double").alias("fare_amount"),
        col_or_null("extra", "double").alias("extra"),
        col_or_null("mta_tax", "double").alias("mta_tax"),
        col_or_null("tip_amount", "double").alias("tip_amount"),
        col_or_null("tolls_amount", "double").alias("tolls_amount"),
        col_or_null("improvement_surcharge", "double").alias("improvement_surcharge"),
        col_or_null("total_amount", "double").alias("total_amount"),
        col_or_null("congestion_surcharge", "double").alias("congestion_surcharge"),
        col_or_null("airport_fee", "double").alias("airport_fee"),
        col_or_null("cbd_congestion_fee", "double").alias("cbd_congestion_fee"),
    )

    if "cbd_congestion_fee" not in available:
        log.info(
            "cbd_congestion_fee absent in source parquet for %s — column will be NULL "
            "(expected for pre-2025 TLC data)",
            filename,
        )

    # Add the same load-metadata columns dbt's loader populates.
    return df.withColumn("source_filename", F.lit(filename)).withColumn(
        "loaded_at", F.current_timestamp()
    )


def read_zones(spark: SparkSession, zones_uri: str):
    """Tiny dim, ~265 rows. Read with explicit schema; broadcast in the join."""
    return spark.read.option("header", True).schema(ZONE_SCHEMA).csv(zones_uri)


# ---------------------------------------------------------------------------
# Staging — port of dbt/models/staging/stg_yellow_trips.sql
# ---------------------------------------------------------------------------


def add_derived_columns(df):
    """Add the same derived columns dbt's stg_yellow_trips computes."""
    return (
        df
        # Fractional minutes via second precision so sub-minute trips don't
        # falsely fail the trip_duration_sane test.
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        # ISO day-of-week: 1=Monday … 7=Sunday. Match dbt's `dayofweekiso`.
        # Spark's F.dayofweek returns 1=Sunday..7=Saturday; convert.
        .withColumn(
            "pickup_dow",
            F.when(F.dayofweek("pickup_ts") == 1, F.lit(7)).otherwise(F.dayofweek("pickup_ts") - 1),
        )
        .withColumn("pickup_year", F.year("pickup_ts"))
        .withColumn("pickup_month", F.month("pickup_ts"))
        .withColumn(
            "tip_pct",
            F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")).otherwise(
                F.lit(None).cast("double")
            ),
        )
    )


def add_dup_rank(df):
    """Match dbt's duplicate_row detection: row_number over the natural key
    set, ordered by source_filename. The 2nd+ occurrence is the duplicate."""
    from pyspark.sql import Window

    w = Window.partitionBy(
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "pu_location_id",
        "do_location_id",
        "fare_amount",
        "total_amount",
        "payment_type",
    ).orderBy("source_filename")
    return df.withColumn("dup_rank", F.row_number().over(w))


def add_invalid_reason(df):
    """Replicate the 12 validity rules from stg_yellow_trips.sql, in order.
    First match wins. Rows with no match get invalid_reason = NULL (i.e. valid)."""
    return df.withColumn(
        "invalid_reason",
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), F.lit("null_timestamp"))
        .when(F.col("pickup_ts") >= F.col("dropoff_ts"), F.lit("pickup_ge_dropoff"))
        .when(F.col("trip_duration_min") > 720, F.lit("duration_out_of_range"))
        .when(
            F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0),
            F.lit("non_positive_distance"),
        )
        .when(F.col("trip_distance") > 200, F.lit("distance_out_of_range"))
        .when(
            (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
            F.lit("negative_fare_or_total"),
        )
        .when(
            (F.col("fare_amount") > 1000) | (F.col("total_amount") > 1000),
            F.lit("excessive_fare_or_total"),
        )
        .when(
            (F.col("tip_amount") > F.col("fare_amount"))
            | (F.col("tip_amount") > F.col("total_amount")),
            F.lit("tip_exceeds_fare_or_total"),
        )
        .when(~F.col("payment_type").isin(1, 2, 3, 4, 5, 6), F.lit("unknown_payment_type"))
        .when(
            F.col("pu_location_id").isNull() | F.col("do_location_id").isNull(),
            F.lit("null_location_id"),
        )
        .when(F.col("dup_rank") > 1, F.lit("duplicate_row"))
        .otherwise(F.lit(None).cast("string")),
    ).withColumn("is_valid", F.col("invalid_reason").isNull())


def add_trip_sk(df):
    """Match dbt's surrogate key: md5(concat_ws('-', null-coalesced columns)).

    Uses dbt_utils.generate_surrogate_key's null sentinel for parity with
    dbt's compiled SQL. Note: byte-equality with dbt-built trip_sk is NOT
    guaranteed (Snowflake vs Spark cast formatting differs on timestamps and
    decimals). Acceptable because Spark and dbt own disjoint month ranges
    (Spark = historical, dbt = live), so there's no key collision in practice.
    """
    NULL_SENTINEL = F.lit("_dbt_utils_surrogate_key_null_")
    cols = [
        F.coalesce(F.col(c).cast("string"), NULL_SENTINEL)
        for c in [
            "vendor_id",
            "pickup_ts",
            "dropoff_ts",
            "pu_location_id",
            "do_location_id",
            "fare_amount",
            "total_amount",
            "source_filename",
        ]
    ]
    return df.withColumn("trip_sk", F.md5(F.concat_ws("-", *cols)))


def stage_trips(trips_raw, zones):
    """Apply derived cols + validity rules + trip_sk + zone enrichment.
    Returns a single df with is_valid flag — split downstream."""
    return (
        trips_raw.transform(add_derived_columns)
        .transform(add_dup_rank)
        .transform(add_invalid_reason)
        .transform(add_trip_sk)
        # Zone enrichment via broadcast (~265-row dim).
        .join(
            F.broadcast(zones).alias("pu"),
            F.col("pu_location_id") == F.col("pu.location_id"),
            how="left",
        )
        .withColumnRenamed("borough", "pu_borough")
        .withColumnRenamed("zone_name", "pu_zone")
        .withColumnRenamed("service_zone", "pu_service_zone")
        .drop("location_id")
        .join(
            F.broadcast(zones).alias("do_z"),
            F.col("do_location_id") == F.col("do_z.location_id"),
            how="left",
        )
        .withColumnRenamed("borough", "do_borough")
        .withColumnRenamed("zone_name", "do_zone")
        .withColumnRenamed("service_zone", "do_service_zone")
        .drop("location_id")
    )


# ---------------------------------------------------------------------------
# Split + write — schemas mirror dbt's int_trips_enriched / int_trips_quarantined
# ---------------------------------------------------------------------------


# Column ORDER matches dbt's int_trips_enriched.sql final SELECT —
# all 19 source columns + derived columns + zone enrichment + load metadata.
FCT_TRIPS_COLUMNS = [
    "trip_sk",
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "pickup_hour",
    "pickup_dow",
    "pickup_year",
    "pickup_month",
    "trip_duration_min",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
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
    "tip_pct",
    "pu_location_id",
    "pu_borough",
    "pu_zone",
    "pu_service_zone",
    "do_location_id",
    "do_borough",
    "do_zone",
    "do_service_zone",
    "source_filename",
    "loaded_at",
]


# Column ORDER matches dbt's int_trips_quarantined.sql final SELECT —
# same column set as FCT_TRIPS plus invalid_reason, so investigators see
# full context for any quarantined row.
FCT_TRIPS_QUARANTINED_COLUMNS = [
    "trip_sk",
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "pickup_hour",
    "pickup_dow",
    "pickup_year",
    "pickup_month",
    "trip_duration_min",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
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
    "tip_pct",
    "pu_location_id",
    "pu_borough",
    "pu_zone",
    "pu_service_zone",
    "do_location_id",
    "do_borough",
    "do_zone",
    "do_service_zone",
    "invalid_reason",
    "source_filename",
    "loaded_at",
]


def write_split(staged, output_uri: str, year: int, month: int) -> None:
    """Split into valid / invalid and write each to its staged-marts/ subpath.

    Files named by (year, month) tag — gives Snowflake's COPY history a
    deterministic filename to dedupe on. Re-running the same month overwrites
    the same file path; Snowflake's COPY history may then re-load it (the
    file's content changed). For a monthly idempotent backfill, this is
    acceptable; if needed, callers can stamp filenames with a run-id.
    """
    if not output_uri.endswith("/"):
        output_uri += "/"

    tag = f"{year:04d}-{month:02d}"

    valid = staged.where(F.col("is_valid")).select(*FCT_TRIPS_COLUMNS).coalesce(1)
    invalid = staged.where(~F.col("is_valid")).select(*FCT_TRIPS_QUARANTINED_COLUMNS).coalesce(1)

    valid_path = f"{output_uri}fct_trips/{tag}/"
    invalid_path = f"{output_uri}fct_trips_quarantined/{tag}/"

    log.info("writing valid → %s", valid_path)
    valid.write.mode("overwrite").parquet(valid_path)

    log.info("writing quarantined → %s", invalid_path)
    invalid.write.mode("overwrite").parquet(invalid_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry: read one TLC month, stage it, write split fact parquet."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="s3://.../raw/")
    parser.add_argument("--output", required=True, help="s3://.../staged-marts/")
    parser.add_argument("--zones", required=True, help="s3://.../spark-scripts/dim_zones.csv")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int, choices=range(1, 13), metavar="{1..12}")
    parser.add_argument(
        "--app-name", default=None, help="Spark app name. Defaults to taxi_stage_<year>_<month>."
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app_name = args.app_name or f"taxi_stage_{args.year}_{args.month:02d}"
    log.info(
        "year=%d month=%d  raw=%s  staged-marts=%s", args.year, args.month, args.input, args.output
    )

    spark = build_session(app_name)
    try:
        trips_raw = read_one_month_raw(spark, args.input, args.year, args.month)
        zones = read_zones(spark, args.zones)
        staged = stage_trips(trips_raw, zones)
        write_split(staged, args.output, args.year, args.month)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
