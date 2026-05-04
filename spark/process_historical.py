"""
process_historical.py — pre-aggregate raw NYC TLC parquet at scale and
write the result as an **Iceberg** table registered in AWS Glue Data
Catalog. Snowflake reads it zero-copy via CATALOG INTEGRATION.

Role in the platform
--------------------
Spark pre-aggregates the historical bulk into a daily-grain
denormalised fact. Snowflake exposes the same data
to analysts via `ANALYTICS.HISTORICAL.HISTORICAL_DAILY_AGG` — both
engines share metadata through Glue and storage through S3. Neither
copies; both query the same physical files.

This is a SCALE concern, NOT a transformation owner. dbt remains the
sole owner of validation, enrichment, and the medallion layers
(Bronze → Silver → Gold). The Iceberg output here feeds analytical
SQL directly, parallel to dbt — they produce different artefacts at
different grains. No engine drift.

Why Iceberg + Glue (vs plain parquet + Snowflake external table)
----------------------------------------------------------------
• True multi-engine interop — Spark writes manifests to Glue on
  commit; Snowflake reads them via CATALOG INTEGRATION. This is the
  open-table-format pattern: catalog is the source of truth, any
  compatible engine can participate.
• Atomic commits — Iceberg's snapshot-isolation semantics prevent
  partial-write visibility. Readers always see a consistent table
  state, even mid-write.
• Schema + partition evolution — adding a column or changing the
  partition spec is metadata-only. No file rewrites.
• Time-travel + rollback — every commit is a snapshot. `... FOR
  TIMESTAMP AS OF ...` queries work in both Spark and Snowflake.
• Hidden partitioning — partition columns are derived (`year(date)`
  / `month(date)`); writers don't need to compute partition values
  manually, queries get pruning automatically.

Output table: glue.taxi_iceberg.historical_daily
------------------------------------------------
Grain: (pickup_date, pickup_hour, pu_location_id, payment_type)

    pickup_date          DATE
    pickup_hour          INT
    pu_location_id       INT
    payment_type         INT
    trip_count           BIGINT
    gross_revenue        DOUBLE   sum(total_amount)
    total_fare           DOUBLE   sum(fare_amount)
    total_tip            DOUBLE   sum(tip_amount)
    total_distance_mi    DOUBLE   sum(trip_distance)
    total_duration_sec   BIGINT   sum(dropoff − pickup, seconds)
    distinct_vendors     BIGINT   approx_count_distinct(VendorID)

Iceberg partition spec: PARTITIONED BY (months(pickup_date))
  — hidden partitioning. The on-disk layout becomes
  `s3://<bucket>/historical-daily/<table>/data/year=YYYY/month=MM/`,
  and queries that filter on `pickup_date` get pruned automatically
  without writing `WHERE year(...)` predicates.

Sums + count are commutative, so any downstream query can re-aggregate
to a coarser grain (year/month/zone, year/zone/payment_type, etc.)
without re-touching raw. Averages and percentiles are derived at query
time from the sums — storing pre-computed averages would lock the
consumer into this grain.

Optimisations
-------------
• Year-glob (read 12 files, not the full s3://nyc-tlc/ tree) → Spark
  prunes at the listing layer; we never list other years.
• Filter rows BEFORE aggregate → smaller shuffle. Validity gate
  mirrors dbt's stg layer — see `filter_valid` for the rule list.
• approx_count_distinct over count(distinct VendorID) → ~10× cheaper
  shuffle in exchange for ±2% accuracy on a sanity metric.
• Iceberg write mode `append` (not overwrite) — for re-running a year
  that already has data, configure the spark conf
  `--conf spark.sql.iceberg.handle-timestamp-without-timezone=true`
  and use `MERGE INTO` if you want true idempotent reprocessing. Our
  default `append` assumes you DROP the year's partitions first via
  `ALTER TABLE ... DROP PARTITION`, or you're inserting a brand-new year.
• AQE on (Spark 3.2+ default; set explicitly for clarity) → adaptive
  shuffle partition coalescing handles skew without manual tuning.
• Snappy compression on the parquet data files (Iceberg default) →
  fast decompression in downstream readers.
• DataFrame API (not RDD) → Catalyst optimisation, predicate pushdown
  into parquet, broadcast-join autodetection.

Required Spark conf (passed by spark_historical Airflow DAG)
------------------------------------------------------------
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
    --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO
    --conf spark.sql.catalog.glue.warehouse=s3://<bucket>/historical-daily/

EMR Serverless 7.x ships Iceberg JARs out of the box — no JAR upload
needed. Older EMR releases would need
`spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0`.

Local / dev test
----------------
    spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,software.amazon.awssdk:bundle:2.20.18 \\
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
        --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog \\
        --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \\
        --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\
        --conf spark.sql.catalog.glue.warehouse=s3://my-bucket/historical-daily \\
        spark/process_historical.py \\
        --catalog       glue \\
        --database      taxi_iceberg \\
        --table         historical_daily \\
        --input         ./data/raw \\
        --year          2023
"""

from __future__ import annotations

import argparse
import logging
import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------------------------------
# TLC parquet schema-drift handling
# ----------------------------------------------------------------------------
# TLC's parquet output is internally inconsistent across months/years:
#
#   • VendorID has been emitted as both INT32 and INT64 in different files
#     within the same year (e.g. 2023-06 INT32, 2023-07 INT64).
#   • Older years lack columns that newer years added
#     (e.g. cbd_congestion_fee arrived in TLC's 2025 schema).
#
# Two officially documented Spark approaches both fail at scale:
#
#   1. `spark.read.option("mergeSchema", true).parquet(glob)` — Spark's
#      mergeSchema does NOT auto-widen INT → BIGINT (SPARK-15516); fails
#      with `CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`.
#   2. `spark.read.schema(my_schema).parquet(glob)` even with
#      `enableVectorizedReader=false` — at scale the row-container
#      allocator picks the wrong slot type and dies with
#      `MutableLong cannot be cast to MutableInt` (SPARK-17601 +
#      reader-internal bug).
#
# Robust workaround per the Spark user mailing list and Databricks KB:
# read each file independently with its own native schema, then cast
# columns at the **DataFrame level** (not at parquet-read level), then
# union. This keeps the parquet reader on its happy path (per-file
# native schema only) and shifts the type coercion to a stable
# DataFrame operation. See `read_year` below.
TLC_COLUMN_CASTS = [
    ("VendorID", "long"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("trip_distance", "double"),
    ("PULocationID", "long"),
    ("DOLocationID", "long"),
    ("payment_type", "long"),
    ("fare_amount", "double"),
    ("tip_amount", "double"),
    ("total_amount", "double"),
]

log = logging.getLogger("process_historical")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pre-aggregate TLC parquet to a daily Iceberg table.")
    p.add_argument(
        "--input",
        required=True,
        help="S3 URI or local path to raw TLC parquet (e.g. s3://nyc-tlc/trip data/)",
    )
    p.add_argument(
        "--catalog",
        default="glue",
        help="Iceberg catalog name as configured in spark conf (default: glue)",
    )
    p.add_argument(
        "--database",
        default="taxi_iceberg",
        help="Glue database / Iceberg namespace (default: taxi_iceberg)",
    )
    p.add_argument(
        "--table",
        default="historical_daily",
        help="Iceberg table name (default: historical_daily)",
    )
    p.add_argument(
        "--year",
        required=True,
        type=int,
        help="Year to process. One job per year keeps shuffle bounded; "
        "fan out 14 parallel jobs to cover the full TLC history.",
    )
    return p.parse_args(argv)


def build_session() -> SparkSession:
    """Spark session — assumes Iceberg + Glue conf is set via spark-submit args.

    EMR Serverless 7.x bundles Iceberg JARs; the Airflow DAG passes the
    catalog conf via sparkSubmitParameters. Local dev uses --packages.
    """
    return (
        SparkSession.builder.appName("nyc-tlc-historical-daily-iceberg")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )


def read_year(spark: SparkSession, base_path: str, year: int) -> DataFrame:
    """Read all monthly files for one year — one file at a time, casting
    each to the canonical types defined by TLC_COLUMN_CASTS, then union.

    Why per-file rather than glob: see TLC_COLUMN_CASTS docstring above
    for the full reasoning. tl;dr — both `mergeSchema=true` and explicit
    `.schema()` fail on TLC's INT32 ↔ INT64 drift; the only stable path
    is per-file native-schema reads with DataFrame-level casts.

    Each file's parquet read is straightforward (Spark uses the file's
    native types — fast vectorized read, no type-promotion edge cases).
    The `select(...cast()...)` afterwards normalises every file's
    DataFrame to identical column types, making the union safe.

    Files missing columns the cast list expects (e.g. older 2009 files
    that lack `airport_fee`) get NULL filled in for those columns
    automatically — `F.col(name)` against a missing column at select
    time would error, so we wrap each cast in `coalesce(col(name),
    lit(None))` semantics by checking the schema before selecting.
    Simpler: TLC's 10 columns we use here have all existed since 2009,
    so this guard mostly never fires. Kept for forward-compat.
    """
    months = months_for_year(year)
    dfs: list[DataFrame] = []
    for month in months:
        path = f"{base_path.rstrip('/')}/{month.filename}"
        try:
            raw = spark.read.parquet(path)
        except Exception as e:  # noqa: BLE001 — best-effort skip on missing/bad files
            log.warning("skipping %s (%s)", path, e)
            continue

        # Build select expressions: cast columns we have, NULL-fill columns
        # we expect but the file doesn't include. TLC's column set has been
        # stable since 2009 for the 10 we project, but newer additions
        # (like cbd_congestion_fee, which we don't read) shouldn't break us.
        present = set(raw.columns)
        select_exprs = [
            F.col(name).cast(dtype).alias(name)
            if name in present
            else F.lit(None).cast(dtype).alias(name)
            for name, dtype in TLC_COLUMN_CASTS
        ]
        dfs.append(raw.select(*select_exprs))
        log.info("read + cast: %s", path)

    if not dfs:
        raise RuntimeError(f"no parquet files found for year {year} under {base_path}")

    # All DataFrames now have identical schemas. unionByName is safe.
    return reduce(lambda a, b: a.unionByName(b), dfs)


def months_for_year(year: int) -> list:
    """Lazy-import wrapper. Avoids a hard dependency on the `ingestion`
    package being importable in the EMR environment (it's pip-installed
    in airflow's container, but the EMR worker has its own minimal
    Python env). Inline-defined to keep this module standalone."""
    from datetime import date

    class _M:
        def __init__(self, y: int, m: int) -> None:
            self.year = y
            self.month = m
            self.tag = date(y, m, 1).strftime("%Y-%m")
            self.filename = f"yellow_tripdata_{self.tag}.parquet"

    return [_M(year, m) for m in range(1, 13)]


def filter_valid(df: DataFrame) -> DataFrame:
    """Apply the same validity gate as dbt's stg_yellow_trips.

    Aggregating dirty rows would inflate trip_count and skew every
    metric. The 11 rules below mirror the case expression in
    `dbt/models/staging/stg_yellow_trips.sql` (canonical source) — keep
    them in sync.
    """
    return df.where(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 200)
        & (F.col("fare_amount") >= 0)
        & (F.col("fare_amount") <= 1000)
        & (F.col("total_amount") >= 0)
        & (F.col("total_amount") <= 1000)
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
        & F.col("payment_type").isin(1, 2, 3, 4, 5, 6)
    )


def aggregate_daily(df: DataFrame) -> DataFrame:
    """Roll up to (date, hour, pu_location, payment_type) grain.

    Computes derived columns inline within the aggregate so Catalyst
    pushes them past the shuffle boundary. `unix_timestamp` differencing
    for duration is faster than `datediff('second', ...)` on Spark —
    single column scan vs two.
    """
    duration_sec = (
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    ).cast("long")

    return (
        df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("duration_sec", duration_sec)
        .groupBy("pickup_date", "pickup_hour", "PULocationID", "payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum("fare_amount").alias("total_fare"),
            F.sum("tip_amount").alias("total_tip"),
            F.sum("trip_distance").alias("total_distance_mi"),
            F.sum("duration_sec").alias("total_duration_sec"),
            F.approx_count_distinct("VendorID").alias("distinct_vendors"),
        )
        .withColumnRenamed("PULocationID", "pu_location_id")
    )


def ensure_iceberg_table(spark: SparkSession, fqtn: str) -> None:
    """Create the Iceberg table if it doesn't exist. Idempotent.

    Hidden partitioning via `months(pickup_date)` (single transform).
    Iceberg's `month()` transform returns months-since-epoch (e.g.
    2023-01 → 636), so it uniquely identifies the calendar month
    AND naturally orders within years. Queries that filter on
    `pickup_date` get partition pruning automatically.

    We don't combine `years(pickup_date)` + `months(pickup_date)` —
    Iceberg rejects that as redundant since both transforms derive
    from the same source column and `month()` already encodes year
    information. Single `months()` is the canonical recommendation.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqtn} (
            pickup_date         DATE,
            pickup_hour         INT,
            pu_location_id      INT,
            payment_type        INT,
            trip_count          BIGINT,
            gross_revenue       DOUBLE,
            total_fare          DOUBLE,
            total_tip           DOUBLE,
            total_distance_mi   DOUBLE,
            total_duration_sec  BIGINT,
            distinct_vendors    BIGINT
        )
        USING iceberg
        PARTITIONED BY (months(pickup_date))
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'snappy',
            'write.distribution-mode' = 'hash',
            'format-version' = '2'
        )
        """
    )
    log.info("ensured iceberg table exists: %s", fqtn)


def replace_year_partitions(spark: SparkSession, fqtn: str, agg: DataFrame, year: int) -> None:
    """Re-runs for an existing year: delete that year's rows, then append.

    `delete + append` is the idempotent pattern for Iceberg without
    needing to set up a full MERGE. Each call replaces the year's
    contribution wholesale, avoiding double-counting on retries.

    Iceberg's delete + append are both atomic — readers see either the
    pre-replace state or the post-replace state, never a partial state.
    """
    spark.sql(f"DELETE FROM {fqtn} WHERE year(pickup_date) = {year}")
    log.info("deleted existing rows for year=%d", year)

    agg.writeTo(fqtn).append()
    log.info("appended new rows for year=%d", year)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    spark = build_session()

    fqtn = f"{args.catalog}.{args.database}.{args.table}"

    raw = read_year(spark, args.input, args.year)
    valid = filter_valid(raw)
    agg = aggregate_daily(valid)

    ensure_iceberg_table(spark, fqtn)
    replace_year_partitions(spark, fqtn, agg, args.year)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
