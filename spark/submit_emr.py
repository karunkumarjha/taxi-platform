"""
Submit ONE EMR Serverless job for a single (year, month) of TLC data.

The Airflow `spark_pipeline` DAG is the canonical driver — it computes the
target month from `logical_date` and calls `main(["--year", "2023", "--month", "6"])`
on its own python path. This module is also usable from the CLI for ad-hoc
single-month submissions:

    python -m spark.submit_emr --year 2023 --month 6

For multi-month backfill, use `make spark-backfill START=YYYY-MM END=YYYY-MM`,
which loops `airflow dags backfill spark_pipeline` — each scheduled run
submits one EMR job. Single-writer-per-month, sequential by `max_active_runs=1`.

Why this module is intentionally small: the DAG is the right place for
multi-month orchestration (retries, observability, scheduling). Replicating
it here as `--years` / `--parallel` flags duplicates surface area without
adding capability the DAG doesn't already have.

Environment (auto-loaded from .env):
    S3_BUCKET, EMR_APPLICATION_ID, EMR_EXEC_ROLE_ARN, AWS_REGION
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("submit_emr")

TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "CANCELLING"}
POLL_SECONDS = 20


# --------------------------------------------------------------------------- #
# Artifact upload                                                             #
# --------------------------------------------------------------------------- #


def upload_artifacts(s3, bucket: str, scripts_prefix: str = "spark-scripts/") -> tuple[str, str]:
    """Upload the PySpark entry point + dim_zones CSV to S3. Returns the URIs."""
    repo_root = Path(__file__).resolve().parent.parent
    script_local = repo_root / "spark" / "process_historical.py"
    zones_local = repo_root / "dbt" / "seeds" / "dim_zones.csv"

    for path in (script_local, zones_local):
        if not path.exists():
            raise SystemExit(f"missing artifact: {path}")

    script_key = f"{scripts_prefix}{script_local.name}"
    zones_key = f"{scripts_prefix}{zones_local.name}"

    log.info("uploading %s → s3://%s/%s", script_local.name, bucket, script_key)
    s3.upload_file(str(script_local), bucket, script_key)

    log.info("uploading %s → s3://%s/%s", zones_local.name, bucket, zones_key)
    s3.upload_file(str(zones_local), bucket, zones_key)

    return f"s3://{bucket}/{script_key}", f"s3://{bucket}/{zones_key}"


# --------------------------------------------------------------------------- #
# Job submission                                                              #
# --------------------------------------------------------------------------- #


def start_job(
    emr,
    *,
    application_id: str,
    exec_role_arn: str,
    script_uri: str,
    zones_uri: str,
    bucket: str,
    year: int,
    month: int,
) -> str:
    """Submit one (year, month) job and return the job_run_id (does NOT wait)."""
    tag = f"{year:04d}-{month:02d}"
    entry_args = [
        "--input",
        f"s3://{bucket}/raw/",
        "--output",
        f"s3://{bucket}/staged-marts/",
        "--zones",
        zones_uri,
        "--year",
        str(year),
        "--month",
        str(month),
    ]

    resp = emr.start_job_run(
        applicationId=application_id,
        executionRoleArn=exec_role_arn,
        name=f"taxi_monthly_{tag}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_uri,
                "entryPointArguments": entry_args,
                "sparkSubmitParameters": (
                    "--conf spark.sql.adaptive.enabled=true "
                    "--conf spark.sql.adaptive.skewJoin.enabled=true "
                    "--conf spark.executor.cores=4 "
                    "--conf spark.executor.memory=8g "
                    "--conf spark.driver.cores=2 "
                    "--conf spark.driver.memory=4g"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{bucket}/spark-logs/{tag}/",
                },
            },
        },
    )
    return resp["jobRunId"]


def wait_for(emr, application_id: str, job_run_id: str, *, label: str = "") -> dict:
    """Poll until the job reaches a terminal state. Returns the final jobRun dict."""
    while True:
        resp = emr.get_job_run(applicationId=application_id, jobRunId=job_run_id)
        state = resp["jobRun"]["state"]
        log.info("%-7s %s → %s", label, job_run_id, state)
        if state in TERMINAL_STATES:
            return resp["jobRun"]
        time.sleep(POLL_SECONDS)


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    """CLI entry: upload artifacts, submit one EMR Serverless job, wait for terminal."""
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--application-id", default=os.environ.get("EMR_APPLICATION_ID"))
    parser.add_argument("--exec-role-arn", default=os.environ.get("EMR_EXEC_ROLE_ARN"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION"))

    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="{1..12}")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    missing = [
        k
        for k, v in {
            "bucket": args.bucket,
            "application_id": args.application_id,
            "exec_role_arn": args.exec_role_arn,
        }.items()
        if not v
    ]
    if missing:
        parser.error(f"missing: {', '.join(missing)} (pass via flag or env)")

    tag = f"{args.year:04d}-{args.month:02d}"
    log.info("submitting EMR job for %s", tag)

    session_kwargs = {"region_name": args.region} if args.region else {}
    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3")
    emr = session.client("emr-serverless")

    script_uri, zones_uri = upload_artifacts(s3, args.bucket)

    job_run_id = start_job(
        emr,
        application_id=args.application_id,
        exec_role_arn=args.exec_role_arn,
        script_uri=script_uri,
        zones_uri=zones_uri,
        bucket=args.bucket,
        year=args.year,
        month=args.month,
    )

    final = wait_for(emr, args.application_id, job_run_id, label=tag)
    if final["state"] != "SUCCESS":
        log.error("%s FAILED: %s", tag, final.get("stateDetails"))
        return 1
    log.info("%s ok", tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
