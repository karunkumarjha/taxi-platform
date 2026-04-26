"""
Stream NYC TLC Yellow Taxi parquet files from CloudFront directly to S3.

Design:
- Raw data lives only in S3 (no local disk cache).
- Smart-resume: for each expected month, HEAD the S3 key; skip if present.
- Streaming upload: requests(stream=True) -> boto3 upload_fileobj, no temp file.
- Idempotent: safe to re-run; only uploads what's missing.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

log = logging.getLogger("ingest_tlc")

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_ZONE_LOOKUP = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_YEAR = 2023


@dataclass(frozen=True)
class Month:
    year: int
    month: int

    @property
    def tag(self) -> str:
        """ISO month tag, e.g. '2023-07'."""
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def filename(self) -> str:
        """TLC parquet filename for this month."""
        return f"yellow_tripdata_{self.tag}.parquet"

    @property
    def url(self) -> str:
        """Public CloudFront URL for this month's parquet."""
        return f"{TLC_BASE}/{self.filename}"


def months_for_year(year: int) -> list[Month]:
    """Return all 12 Month entries for the given year."""
    return [Month(year, m) for m in range(1, 13)]


def parse_months(value: str) -> list[Month]:
    """Accept '2023', '2023-01', or a comma-separated mix."""
    out: list[Month] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            y, m = token.split("-")
            out.append(Month(int(y), int(m)))
        else:
            out.extend(months_for_year(int(token)))
    return out


def s3_key(month: Month, prefix: str) -> str:
    """Build the S3 key under `prefix` for this month's parquet."""
    return f"{prefix.rstrip('/')}/{month.filename}"


def object_exists(s3, bucket: str, key: str) -> bool:
    """HEAD the S3 object; True if present, False on 404, raise on other errors."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def is_published_on_tlc(url: str) -> bool:
    """HEAD-check TLC's CloudFront for a parquet URL.

    Returns True if the file is published, False if 404. Other HTTP errors
    (5xx, connection issues) re-raise — those are real failures, not "month
    not published yet".

    Lets `make ingest MONTHS=2026` (a year only partially published) succeed
    on what's available and skip the not-yet-published months without exiting.
    """
    resp = requests.head(url, timeout=10, allow_redirects=True)
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    return True


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def stream_month_to_s3(s3, bucket: str, key: str, url: str) -> int:
    """Stream a single parquet file from CloudFront → S3 without touching disk.

    Returns bytes uploaded. boto3's upload_fileobj handles multipart under the hood,
    which matters because TLC monthly files are ~50MB each (fine on single-part)
    but keeps the code robust to larger historical files (Phase 2).
    """
    log.info("downloading %s → s3://%s/%s", url, bucket, key)
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        # raw.read() streams without decoding; upload_fileobj handles chunking.
        resp.raw.decode_content = True
        s3.upload_fileobj(
            resp.raw,
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
    head = s3.head_object(Bucket=bucket, Key=key)
    return int(head["ContentLength"])


def ingest_missing(
    months: list[Month],
    bucket: str,
    prefix: str = "raw/",
    *,
    s3_client=None,
) -> list[str]:
    """Upload only months not already present in s3://<bucket>/<prefix>.

    Returns the list of S3 keys that were newly uploaded.
    """
    s3 = s3_client or boto3.client("s3")
    uploaded: list[str] = []
    for month in months:
        key = s3_key(month, prefix)
        if object_exists(s3, bucket, key):
            log.info("skip %s — already in S3", key)
            continue
        if not is_published_on_tlc(month.url):
            # TLC publishes month M roughly 2 months after M ends. A 404 here
            # means we asked for a future / not-yet-published month — skip
            # quietly and move on (rather than failing the whole batch).
            log.warning("skip %s — TLC has not published %s yet", key, month.url)
            continue
        size = stream_month_to_s3(s3, bucket, key, month.url)
        log.info("uploaded %s (%.1f MB)", key, size / 1_048_576)
        uploaded.append(key)
    return uploaded


def ensure_zone_lookup(local_path: str) -> None:
    """Fetch taxi_zone_lookup.csv to disk for use as a dbt seed.

    The zone lookup is small + static — committing it as a seed is the idiomatic dbt
    pattern, so this step runs at most once (when the seed file is missing).
    """
    if os.path.exists(local_path):
        log.info("zone lookup already present at %s", local_path)
        return
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    log.info("fetching %s → %s", TLC_ZONE_LOOKUP, local_path)
    resp = requests.get(TLC_ZONE_LOOKUP, timeout=30)
    resp.raise_for_status()
    with open(local_path, "wb") as fh:
        fh.write(resp.content)


def main(argv: list[str] | None = None) -> int:
    """CLI entry: parse args, fetch zone lookup, ingest missing months to S3."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months",
        default=str(DEFAULT_YEAR),
        help="Months to ingest. Accepts '2023', '2023-07', or a mix: '2023-01,2023-02,2024'.",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET"),
        help="S3 bucket (default: $S3_BUCKET).",
    )
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_RAW_PREFIX", "raw/"),
        help="S3 prefix (default: $S3_RAW_PREFIX or 'raw/').",
    )
    parser.add_argument(
        "--zone-lookup",
        default="dbt/seeds/taxi_zones.csv",
        help="Local path to cache taxi_zone_lookup.csv for dbt seeds.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.bucket:
        parser.error("--bucket is required (or set $S3_BUCKET)")

    months = parse_months(args.months)
    log.info("ingest start bucket=%s prefix=%s months=%d", args.bucket, args.prefix, len(months))
    started = datetime.utcnow()

    ensure_zone_lookup(args.zone_lookup)
    uploaded = ingest_missing(months, args.bucket, args.prefix)

    elapsed = (datetime.utcnow() - started).total_seconds()
    log.info(
        "ingest done uploaded=%d skipped=%d elapsed=%.1fs",
        len(uploaded),
        len(months) - len(uploaded),
        elapsed,
    )

    # Emit uploaded keys to stdout, one per line, for downstream tooling (Airflow XCom).
    for key in uploaded:
        print(key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
