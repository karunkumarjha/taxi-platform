"""
process_historical.py — pre-aggregate raw NYC TLC parquet at scale and write
three Iceberg tables registered in AWS Glue. Snowflake reads zero-copy via
CATALOG INTEGRATION.

dbt remains sole owner of validation/transformation; this script writes
scale-time pre-aggregates that mirror the dbt mart grains so the same
business questions (Q1–Q4) can be answered across the full historical
range.

Three outputs (one Spark application, one source read, three writes):

    glue.taxi_iceberg.daily_agg          — Q1, Q2
        grain: (pickup_date, pickup_hour, pu_location_id, payment_type)
        cols:  trip_count, gross_revenue, total_fare, total_tip,
               total_distance_mi, total_duration_sec, distinct_vendors

    glue.taxi_iceberg.supply_gaps    — Q3
        grain: (pickup_date, pu_location_id)
        cols:  trip_count, longest_gap_min, avg_gap_min,
               gaps_gt_1h, gaps_gt_3h, first_pickup_ts, last_pickup_ts
        Computed via LAG(pickup_ts) over (date, zone) — never crosses
        day boundaries, so partition+sort within (date, zone) is the
        natural unit for Spark.

    glue.taxi_iceberg.tip_behaviour  — Q4
        grain: (pickup_date, pu_location_id, distance_bucket, payment_type)
        cols:  trip_count, total_fare, total_tip, total_distance_mi
        distance_bucket mirrors agg_zone_tip_behaviour_monthly:
            0-1mi / 1-3mi / 3-5mi / 5-10mi / 10-20mi / 20mi+

All three tables are partitioned by `months(pickup_date)` (Iceberg hidden
partitioning) — queries that filter on `pickup_date` get pruning automatically.
The (date, zone) supply-gaps grain still gets month-pruned because every
date row implies its month.

Per-year idempotency: every run does DELETE WHERE year(pickup_date) = $year
THEN append, against each of the three tables. Safe to retry; never
double-counts.

Spark conf required (passed by spark_historical Airflow DAG):

    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
    --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO
    --conf spark.sql.catalog.glue.warehouse=s3://<bucket>/historical-daily/

EMR Serverless 7.x ships Iceberg JARs out of the box. Local dev:

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
        --input         ./data/raw \\
        --year          2023
"""

from __future__ import annotations

import argparse
import logging
import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# TLC parquet schema-drift handling — VendorID flips INT32 ↔ INT64 across
# files in the same year (SPARK-15516). Per-file native read + DataFrame-level
# casts is the only stable path; mergeSchema and explicit .schema() both fail
# on TLC's drift.
TLC_COLUMN_CASTS = [
    # Natural-key columns (5) — partition key for dedupe + aggregation join keys.
    ("VendorID", "long"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("PULocationID", "long"),
    ("DOLocationID", "long"),
    # Aggregation / validity-rule columns.
    ("trip_distance", "double"),
    ("payment_type", "long"),
    ("fare_amount", "double"),
    ("tip_amount", "double"),
    ("total_amount", "double"),
    # Remaining non-key columns — present so the dedupe tiebreak ladder can
    # rank truly-identical rows deterministically (must match the snapshot's
    # final tiebreak in dbt/snapshots/snp_yellow_trips.sql column-for-column).
    ("passenger_count", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
    ("cbd_congestion_fee", "double"),
    ("RatecodeID", "double"),
    ("store_and_fwd_flag", "string"),
]

# Three output tables — fixed names. The DAG and Snowflake-side DDL
# both reference these names, so changing them here requires a coordinated
# change in airflow/dags/spark_historical.py.
TABLE_DAILY = "daily_agg"
TABLE_SUPPLY_GAPS = "supply_gaps"
TABLE_TIP_BEHAVIOUR = "tip_behaviour"

log = logging.getLogger("process_historical")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pre-aggregate TLC parquet to 3 Iceberg tables.")
    p.add_argument(
        "--input",
        required=True,
        help="S3 URI or local path to raw TLC parquet (e.g. s3://my-bucket/raw/)",
    )
    p.add_argument("--catalog", default="glue", help="Iceberg catalog (default: glue)")
    p.add_argument(
        "--database",
        default="taxi_iceberg",
        help="Glue database / Iceberg namespace (default: taxi_iceberg)",
    )
    p.add_argument(
        "--year",
        required=True,
        type=int,
        help="Year to process. One job = one year, idempotent via DELETE+append.",
    )
    return p.parse_args(argv)


def build_session() -> SparkSession:
    """Spark session — Iceberg + Glue conf comes from spark-submit args."""
    return (
        SparkSession.builder.appName("nyc-tlc-historical-iceberg")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )


def months_for_year(year: int) -> list:
    """Inline so this module is standalone in EMR's minimal Python env."""
    from datetime import date

    class _M:
        def __init__(self, y: int, m: int) -> None:
            self.year = y
            self.month = m
            self.tag = date(y, m, 1).strftime("%Y-%m")
            self.filename = f"yellow_tripdata_{self.tag}.parquet"

    return [_M(year, m) for m in range(1, 13)]


def read_year(spark: SparkSession, base_path: str, year: int) -> DataFrame:
    """Read 12 monthly parquet files, cast each to canonical types, union.

    Per-file (not glob) reads + DataFrame-level casts are the only stable
    path through TLC's INT32 ↔ INT64 schema drift — see TLC_COLUMN_CASTS.
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

        # Files missing newer columns get NULL-filled; keeps forward-compat
        # without breaking on older years that lack what newer years have.
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
    return reduce(lambda a, b: a.unionByName(b), dfs)


def filter_valid(df: DataFrame) -> DataFrame:
    """Apply the same validity gate as dbt's stg_yellow_trips. Keep in sync
    with dbt/models/staging/stg_yellow_trips.sql.

    Order doesn't matter for the boolean filter (the ordering in the dbt
    case-when only affects which `invalid_reason` label gets attached;
    here we only care whether the row passes or not).
    """
    duration_min = (
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    ) / 60.0
    return df.where(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
        & (duration_min <= 720)  # duration_out_of_range
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 200)
        & (F.col("fare_amount") >= 0)
        & (F.col("fare_amount") <= 1000)
        & (F.col("total_amount") >= 0)
        & (F.col("total_amount") <= 1000)
        & ~(F.col("tip_amount") > F.col("fare_amount"))  # tip_exceeds_fare_or_total (a)
        & ~(F.col("tip_amount") > F.col("total_amount"))  # tip_exceeds_fare_or_total (b)
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
        & F.col("payment_type").isin(1, 2, 3, 4, 5, 6)
    )


def dedupe_natural_key(df: DataFrame) -> DataFrame:
    """Keep one row per natural key (VendorID, pickup_ts, dropoff_ts,
    PULocationID, DOLocationID) — same 5 cols as dbt's trip_bk.

    Tiebreak ordering MUST match dbt/snapshots/snp_yellow_trips.sql.
    Both engines deterministically pick the copy most likely to pass
    stg_yellow_trips' validity gate, so mixed-validity TLC duplicates
    (e.g. one row with payment_type=1 and a ghost row with payment_type=0
    for the same trip) collapse to the legitimate copy on both sides.
    Without an aligned tiebreak the two engines' default orders
    (Snowflake micro-partition vs Spark DataFrame partition) disagree
    and produce systematic drift between MARTS.AGG_* and HISTORICAL.*.
    """
    pass_payment = F.when(F.col("payment_type").isin(1, 2, 3, 4, 5, 6), 0).otherwise(1)
    pass_fare = F.when((F.col("fare_amount") >= 0) & (F.col("fare_amount") <= 1000), 0).otherwise(1)
    pass_total = F.when(
        (F.col("total_amount") >= 0) & (F.col("total_amount") <= 1000), 0
    ).otherwise(1)
    pass_distance = F.when(
        (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 200), 0
    ).otherwise(1)
    pass_tip = F.when(
        (F.col("tip_amount") <= F.col("fare_amount"))
        & (F.col("tip_amount") <= F.col("total_amount")),
        0,
    ).otherwise(1)

    w = Window.partitionBy(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
    ).orderBy(
        # Match snp_yellow_trips' `_loaded_at desc, _ingest_batch_id desc`
        # equivalent: Spark reads each parquet once, so there is no per-row
        # _loaded_at — input_file_name is the analogous "load source" key.
        F.input_file_name(),
        # Content-aware tiebreak — 0 = passes rule, sorts before 1.
        pass_payment,
        pass_fare,
        pass_total,
        pass_distance,
        pass_tip,
        # Final stable tiebreak — column order MUST match the matching
        # `nulls last` ladder in dbt/snapshots/snp_yellow_trips.sql so two
        # truly-byte-identical rows are the only case where dedup falls
        # back to engine-internal order (where the pick genuinely doesn't
        # matter because the rows are interchangeable).
        F.col("payment_type").asc_nulls_last(),
        F.col("fare_amount").asc_nulls_last(),
        F.col("tip_amount").asc_nulls_last(),
        F.col("total_amount").asc_nulls_last(),
        F.col("passenger_count").asc_nulls_last(),
        F.col("trip_distance").asc_nulls_last(),
        F.col("extra").asc_nulls_last(),
        F.col("mta_tax").asc_nulls_last(),
        F.col("tolls_amount").asc_nulls_last(),
        F.col("improvement_surcharge").asc_nulls_last(),
        F.col("congestion_surcharge").asc_nulls_last(),
        F.col("airport_fee").asc_nulls_last(),
        F.col("cbd_congestion_fee").asc_nulls_last(),
        F.col("RatecodeID").asc_nulls_last(),
        F.col("store_and_fwd_flag").asc_nulls_last(),
    )
    return (
        df.withColumn("_dup_rank", F.row_number().over(w))
        .where(F.col("_dup_rank") == 1)
        .drop("_dup_rank")
    )


# ----------------------------------------------------------------------------
# Aggregations — all three operate on the same filtered DataFrame
# ----------------------------------------------------------------------------


def aggregate_daily(df: DataFrame) -> DataFrame:
    """Q1+Q2 grain: (date, hour, zone, payment_type)."""
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


def aggregate_supply_gaps(df: DataFrame) -> DataFrame:
    """Q3 grain: (date, zone). LAG over pickup_ts within (date, zone).

    Mirrors dbt/models/marts/agg_zone_supply_gaps_daily.sql. The window
    partition is (pickup_date, pu_location_id) — gaps never span days,
    so each (date, zone) shuffle key is independent and parallelises well.
    """
    w = Window.partitionBy("pickup_date", "PULocationID").orderBy("tpep_pickup_datetime")
    with_gap = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime")).withColumn(
        "gap_min",
        (
            F.unix_timestamp("tpep_pickup_datetime")
            - F.unix_timestamp(F.lag("tpep_pickup_datetime").over(w))
        )
        / 60.0,
    )
    return (
        with_gap.groupBy("pickup_date", "PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.max("gap_min").alias("longest_gap_min"),
            F.avg("gap_min").alias("avg_gap_min"),
            F.sum(F.when(F.col("gap_min") > 60, 1).otherwise(0)).cast("long").alias("gaps_gt_1h"),
            F.sum(F.when(F.col("gap_min") > 180, 1).otherwise(0)).cast("long").alias("gaps_gt_3h"),
            F.min("tpep_pickup_datetime").alias("first_pickup_ts"),
            F.max("tpep_pickup_datetime").alias("last_pickup_ts"),
        )
        .withColumnRenamed("PULocationID", "pu_location_id")
    )


def aggregate_tip_behaviour(df: DataFrame) -> DataFrame:
    """Q4 grain: (date, zone, distance_bucket, payment_type). Mirrors
    agg_zone_tip_behaviour_monthly's bucket boundaries exactly."""
    bucket = (
        F.when(F.col("trip_distance") <= 1, "0-1mi")
        .when(F.col("trip_distance") <= 3, "1-3mi")
        .when(F.col("trip_distance") <= 5, "3-5mi")
        .when(F.col("trip_distance") <= 10, "5-10mi")
        .when(F.col("trip_distance") <= 20, "10-20mi")
        .otherwise("20mi+")
    )
    return (
        df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("distance_bucket", bucket)
        .groupBy("pickup_date", "PULocationID", "distance_bucket", "payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("total_fare"),
            F.sum("tip_amount").alias("total_tip"),
            F.sum("trip_distance").alias("total_distance_mi"),
        )
        .withColumnRenamed("PULocationID", "pu_location_id")
    )


# ----------------------------------------------------------------------------
# Iceberg DDL + per-year replace
# ----------------------------------------------------------------------------


def ensure_iceberg_tables(spark: SparkSession, db_fqn: str) -> None:
    """Create the three Iceberg tables if they don't exist. Idempotent.

    All three use months(pickup_date) hidden partitioning so date-filtered
    queries get pruning automatically. format-version=2 enables the
    optimised delete + write path Iceberg uses for replace_year_partitions.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_fqn}.{TABLE_DAILY} (
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
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_fqn}.{TABLE_SUPPLY_GAPS} (
            pickup_date         DATE,
            pu_location_id      INT,
            trip_count          BIGINT,
            longest_gap_min     DOUBLE,
            avg_gap_min         DOUBLE,
            gaps_gt_1h          BIGINT,
            gaps_gt_3h          BIGINT,
            first_pickup_ts     TIMESTAMP,
            last_pickup_ts      TIMESTAMP
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
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_fqn}.{TABLE_TIP_BEHAVIOUR} (
            pickup_date         DATE,
            pu_location_id      INT,
            distance_bucket     STRING,
            payment_type        INT,
            trip_count          BIGINT,
            total_fare          DOUBLE,
            total_tip           DOUBLE,
            total_distance_mi   DOUBLE
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
    log.info("ensured all 3 iceberg tables exist under %s", db_fqn)


def replace_year_partitions(spark: SparkSession, fqtn: str, agg: DataFrame, year: int) -> None:
    """Per-year DELETE + append. Idempotent across reruns. Each step is an
    atomic Iceberg snapshot — readers never see a partial state."""
    spark.sql(f"DELETE FROM {fqtn} WHERE year(pickup_date) = {year}")
    log.info("deleted existing rows for year=%d in %s", year, fqtn)
    agg.writeTo(fqtn).append()
    log.info("appended new rows for year=%d to %s", year, fqtn)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    spark = build_session()

    db_fqn = f"{args.catalog}.{args.database}"

    # One source read, dedupe, validity filter — cached so all three
    # aggregations share the same underlying scan + dedupe + filter
    # pipeline. Without cache(), Spark would redo all three steps per
    # aggregate.
    #
    # Operation ORDER matches dbt: snapshot dedupe first, then
    # stg_yellow_trips applies the validity gate. Reversing the order
    # would let Spark systematically retain the valid copy of a
    # natural-key-duplicate pair where dbt may have arbitrarily picked
    # the invalid copy and quarantined the trip — inflating Spark's
    # output by the count of mixed-valid/invalid duplicate pairs (the
    # most common TLC duplicate pattern: same 5-col natural key, one
    # row with payment_type=1, one with payment_type=0).
    raw = read_year(spark, args.input, args.year)
    valid = filter_valid(dedupe_natural_key(raw)).cache()

    ensure_iceberg_tables(spark, db_fqn)

    replace_year_partitions(spark, f"{db_fqn}.{TABLE_DAILY}", aggregate_daily(valid), args.year)
    replace_year_partitions(
        spark, f"{db_fqn}.{TABLE_SUPPLY_GAPS}", aggregate_supply_gaps(valid), args.year
    )
    replace_year_partitions(
        spark, f"{db_fqn}.{TABLE_TIP_BEHAVIOUR}", aggregate_tip_behaviour(valid), args.year
    )

    valid.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
