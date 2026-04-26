"""
Load staged parquet files from the Snowflake external stage into RAW.YELLOW_TRIPDATA.

Idempotency comes from Snowflake: COPY INTO tracks every loaded filename for 64 days
and skips files it has already ingested. No application-level bookkeeping required.

This module is intentionally thin — the Terraform module creates the database,
schema, warehouse, file format, storage integration, and external stage. All this
script does is ensure the target table exists and run COPY INTO.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("load_snowflake")

# Column definitions for the target table. Matches TLC Yellow 2023 parquet schema.
# MATCH_BY_COLUMN_NAME handles case + ordering differences between the file and the table.
TARGET_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.{schema}.{table} (
    VendorID                INTEGER,
    tpep_pickup_datetime    TIMESTAMP_NTZ,
    tpep_dropoff_datetime   TIMESTAMP_NTZ,
    passenger_count         NUMBER(10, 2),
    trip_distance           NUMBER(18, 4),
    RatecodeID              NUMBER(10, 2),
    store_and_fwd_flag      STRING,
    PULocationID            INTEGER,
    DOLocationID            INTEGER,
    payment_type            INTEGER,
    fare_amount             NUMBER(18, 4),
    extra                   NUMBER(18, 4),
    mta_tax                 NUMBER(18, 4),
    tip_amount              NUMBER(18, 4),
    tolls_amount            NUMBER(18, 4),
    improvement_surcharge   NUMBER(18, 4),
    total_amount            NUMBER(18, 4),
    congestion_surcharge    NUMBER(18, 4),
    airport_fee             NUMBER(18, 4),
    -- load metadata
    _source_filename        STRING,
    _loaded_at              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Landing table for TLC Yellow trip parquet — loaded via COPY INTO from @S3_TLC_STAGE'
"""

COPY_SQL = """
COPY INTO {db}.{schema}.{table} (
    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    _source_filename
)
FROM (
    SELECT
        $1:VendorID::INTEGER,
        -- Parquet stores TLC timestamps as INT64 microseconds since epoch.
        -- Snowflake's $1:field returns that as a NUMBER (variant), and a
        -- direct ::TIMESTAMP_NTZ cast mis-interprets the unit and puts
        -- every row in 1970. Divide by 1,000,000 to get seconds, then
        -- TO_TIMESTAMP_NTZ does the right thing.
        TO_TIMESTAMP_NTZ(($1:tpep_pickup_datetime::BIGINT) / 1000000),
        TO_TIMESTAMP_NTZ(($1:tpep_dropoff_datetime::BIGINT) / 1000000),
        $1:passenger_count::NUMBER(10, 2),
        $1:trip_distance::NUMBER(18, 4),
        $1:RatecodeID::NUMBER(10, 2),
        $1:store_and_fwd_flag::STRING,
        $1:PULocationID::INTEGER,
        $1:DOLocationID::INTEGER,
        $1:payment_type::INTEGER,
        $1:fare_amount::NUMBER(18, 4),
        $1:extra::NUMBER(18, 4),
        $1:mta_tax::NUMBER(18, 4),
        $1:tip_amount::NUMBER(18, 4),
        $1:tolls_amount::NUMBER(18, 4),
        $1:improvement_surcharge::NUMBER(18, 4),
        $1:total_amount::NUMBER(18, 4),
        $1:congestion_surcharge::NUMBER(18, 4),
        $1:airport_fee::NUMBER(18, 4),
        METADATA$FILENAME
    FROM @{db}.{schema}.{stage}
)
FILE_FORMAT = (FORMAT_NAME = '{db}.{schema}.{file_format}')
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE
"""


def connect(cfg: dict) -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection from the env-derived cfg dict."""
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        role=cfg["role"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
    )


def ensure_table(conn, cfg: dict) -> None:
    """Create RAW.YELLOW_TRIPDATA if it doesn't already exist."""
    ddl = TARGET_TABLE_DDL.format(
        db=cfg["database"], schema=cfg["schema"], table=cfg["table"]
    )
    with conn.cursor() as cur:
        cur.execute(ddl)
        log.info("ensured table %s.%s.%s", cfg["database"], cfg["schema"], cfg["table"])


def copy_from_stage(conn, cfg: dict) -> list[tuple]:
    """Run COPY INTO from the external stage; returns the per-file result rows."""
    sql = COPY_SQL.format(
        db=cfg["database"],
        schema=cfg["schema"],
        table=cfg["table"],
        stage=cfg["stage"],
        file_format=cfg["file_format"],
    )
    with conn.cursor() as cur:
        log.info("running COPY INTO %s.%s.%s", cfg["database"], cfg["schema"], cfg["table"])
        cur.execute(sql)
        rows = cur.fetchall()
    for row in rows:
        log.info("copy result: %s", row)
    return rows


def _cfg_from_env() -> dict:
    """Read all SNOWFLAKE_* env vars into a cfg dict; SystemExit on missing required keys."""
    required = {
        "account": "SNOWFLAKE_ACCOUNT",
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",
        "role": "SNOWFLAKE_ROLE",
        "warehouse": "SNOWFLAKE_WAREHOUSE",
    }
    cfg = {}
    missing = []
    for key, env in required.items():
        val = os.environ.get(env)
        if not val:
            missing.append(env)
        cfg[key] = val
    if missing:
        raise SystemExit(f"missing required env vars: {', '.join(missing)}")
    cfg["database"] = os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS")
    cfg["schema"] = os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW")
    cfg["stage"] = os.environ.get("SNOWFLAKE_STAGE", "S3_TLC_STAGE")
    cfg["file_format"] = os.environ.get("SNOWFLAKE_FILE_FORMAT", "PARQUET_FF")
    cfg["table"] = os.environ.get("SNOWFLAKE_RAW_TABLE", "YELLOW_TRIPDATA")
    return cfg


def main(argv: list[str] | None = None) -> int:
    """CLI entry: ensure the table exists then COPY INTO from the external stage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = _cfg_from_env()
    conn = connect(cfg)
    try:
        ensure_table(conn, cfg)
        copy_from_stage(conn, cfg)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
