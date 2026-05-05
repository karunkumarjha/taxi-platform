"""
Fingerprint-based drift detection for TLC source files.

Compares CloudFront ETag vs RAW.SOURCE_FINGERPRINTS. Returns one of:
    new        — never ingested
    unchanged  — same ETag as last build
    drifted    — TLC republished → caller should partition-replace before re-ingest

ETag is metadata-only (CloudFront forwards S3's pre-computed value), so
fingerprinting costs one HEAD per month — no download.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import Literal

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("detect_drift")

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DriftStatus = Literal["new", "unchanged", "drifted"]


@dataclass(frozen=True)
class Fingerprint:
    filename: str
    etag: str
    last_modified: str | None
    content_length: int | None


def fetch_tlc_fingerprint(filename: str) -> Fingerprint | None:
    """HEAD CloudFront for `filename`. Return None if not yet published (404)."""
    url = f"{TLC_BASE}/{filename}"
    resp = requests.head(url, timeout=10, allow_redirects=True)
    if resp.status_code == 404:
        log.warning("tlc has not published %s yet", filename)
        return None
    resp.raise_for_status()
    etag = resp.headers.get("ETag", "").strip('"')
    if not etag:
        raise RuntimeError(f"no ETag header on {url} — cannot fingerprint")
    cl = resp.headers.get("Content-Length")
    return Fingerprint(
        filename=filename,
        etag=etag,
        last_modified=resp.headers.get("Last-Modified"),
        content_length=int(cl) if cl else None,
    )


def ensure_fingerprint_table(sf, raw_schema: str = "RAW") -> None:
    sf.cursor().execute(
        f"""
        CREATE TABLE IF NOT EXISTS {raw_schema}.SOURCE_FINGERPRINTS (
            filename         STRING       NOT NULL PRIMARY KEY,
            etag             STRING       NOT NULL,
            last_modified    STRING,
            content_length   BIGINT,
            recorded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def get_stored_fingerprint(sf, filename: str, raw_schema: str = "RAW") -> str | None:
    cur = sf.cursor()
    cur.execute(
        f"SELECT etag FROM {raw_schema}.SOURCE_FINGERPRINTS WHERE filename = %s",
        (filename,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_fingerprint(sf, fp: Fingerprint, raw_schema: str = "RAW") -> None:
    sf.cursor().execute(
        f"""
        MERGE INTO {raw_schema}.SOURCE_FINGERPRINTS t
        USING (SELECT %s AS filename, %s AS etag, %s AS last_modified, %s AS content_length) s
        ON t.filename = s.filename
        WHEN MATCHED THEN UPDATE SET
            etag = s.etag,
            last_modified = s.last_modified,
            content_length = s.content_length,
            recorded_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (filename, etag, last_modified, content_length)
            VALUES (s.filename, s.etag, s.last_modified, s.content_length)
        """,
        (fp.filename, fp.etag, fp.last_modified, fp.content_length),
    )


def detect_drift(
    sf, filename: str, raw_schema: str = "RAW"
) -> tuple[DriftStatus, Fingerprint | None]:
    """Compare current ETag vs stored ETag for `filename`.

    Caller upserts the fingerprint only after a successful build, so a
    failed run leaves the stored ETag untouched and the next run retries.
    """
    ensure_fingerprint_table(sf, raw_schema)
    current = fetch_tlc_fingerprint(filename)
    if current is None:
        return "unchanged", None
    stored = get_stored_fingerprint(sf, filename, raw_schema)
    if stored is None:
        return "new", current
    if stored == current.etag:
        return "unchanged", current
    log.warning("DRIFT %s: stored=%s current=%s", filename, stored, current.etag)
    return "drifted", current


def partition_replace(
    sf,
    filename: str,
    *,
    raw_table: str = "RAW.YELLOW_TRIPDATA",
    snapshot_table: str = "SNAPSHOTS.SNP_YELLOW_TRIPS",
) -> None:
    """Wipe raw + snapshot rows for `filename` before a corrected re-ingest.

    Called only on 'drifted' status. Pre-correction SCD history for the
    affected month is lost — deliberate trade, documented in the README.
    """
    cur = sf.cursor()
    cur.execute(f"DELETE FROM {raw_table} WHERE _source_filename = %s", (filename,))
    raw_deleted = cur.rowcount
    cur.execute(f"DELETE FROM {snapshot_table} WHERE _source_filename = %s", (filename,))
    snap_deleted = cur.rowcount
    log.warning(
        "partition replace %s: raw=%d, snapshot=%d rows wiped",
        filename,
        raw_deleted,
        snap_deleted,
    )


def main(argv: list[str] | None = None) -> int:
    """CLI: python -m ingestion.detect_drift --year 2023 --month 3"""
    import argparse

    from ingestion.load_snowflake import _cfg_from_env, connect

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    filename = f"yellow_tripdata_{args.year:04d}-{args.month:02d}.parquet"
    cfg = _cfg_from_env()
    sf = connect(cfg)
    try:
        raw_schema = os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW")
        status, _ = detect_drift(sf, filename, raw_schema=raw_schema)
        print(status)
    finally:
        sf.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
