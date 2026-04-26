"""
Submit one EMR Serverless job per (year, month) of TLC data.

Why one job per month:
    Spark job grain matches output partition grain (year, month). Failures are
    isolated to a single month; backfills are a deterministic loop. EMR
    Serverless's pre-init capacity = 0 means we pay nothing for idle time
    between submissions.

Submission modes:
    Sequential (default) — wait for each job to finish before submitting next.
                          Safe, deterministic, easy to debug.
    Parallel             — fire all jobs at once, poll for terminal state.
                          Faster but bounded by the EMR app's max capacity.

Usage:
    # Single month
    python -m spark.submit_emr --year 2023 --month 1

    # Whole year (sequential, default)
    python -m spark.submit_emr --year 2023

    # Whole year (parallel)
    python -m spark.submit_emr --year 2023 --parallel

    # Multi-year backfill
    python -m spark.submit_emr --years 2022,2023

Environment (auto-loaded from .env):
    S3_BUCKET, EMR_APPLICATION_ID, EMR_EXEC_ROLE_ARN, AWS_REGION
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("submit_emr")

TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "CANCELLING"}
POLL_SECONDS = 20


@dataclass(frozen=True)
class MonthRun:
    year: int
    month: int

    @property
    def tag(self) -> str:
        """ISO month tag, e.g. '2023-07'."""
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def job_name(self) -> str:
        """EMR job-run name for this month."""
        return f"taxi_monthly_{self.tag}"


# --------------------------------------------------------------------------- #
# Artifact upload (one-time per run, shared across all month jobs)            #
# --------------------------------------------------------------------------- #


def upload_artifacts(s3, bucket: str, scripts_prefix: str = "spark-scripts/") -> tuple[str, str]:
    """Upload the PySpark entry point + dim_zones CSV to S3. Returns the URIs.

    These are uploaded ONCE per submit_emr run, then re-used by every month job.
    """
    repo_root = Path(__file__).resolve().parent.parent
    script_local = repo_root / "spark" / "process_historical.py"
    zones_local = repo_root / "dbt" / "seeds" / "dim_zones.csv"

    for path in (script_local, zones_local):
        if not path.exists():
            raise SystemExit(f"missing artifact: {path}")

    script_key = f"{scripts_prefix}{script_local.name}"
    zones_key  = f"{scripts_prefix}{zones_local.name}"

    log.info("uploading %s → s3://%s/%s", script_local.name, bucket, script_key)
    s3.upload_file(str(script_local), bucket, script_key)

    log.info("uploading %s → s3://%s/%s", zones_local.name, bucket, zones_key)
    s3.upload_file(str(zones_local), bucket, zones_key)

    return f"s3://{bucket}/{script_key}", f"s3://{bucket}/{zones_key}"


# --------------------------------------------------------------------------- #
# Per-month job submission                                                    #
# --------------------------------------------------------------------------- #


def start_one_month(
    emr,
    *,
    application_id: str,
    exec_role_arn: str,
    script_uri: str,
    zones_uri: str,
    bucket: str,
    run: MonthRun,
) -> str:
    """Submit one month and return the job_run_id (does NOT wait)."""
    entry_args = [
        "--input",  f"s3://{bucket}/raw/",
        "--output", f"s3://{bucket}/analytics/daily_zone_aggregates/",
        "--zones",  zones_uri,
        "--year",   str(run.year),
        "--month",  str(run.month),
    ]

    resp = emr.start_job_run(
        applicationId=application_id,
        executionRoleArn=exec_role_arn,
        name=run.job_name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_uri,
                "entryPointArguments": entry_args,
                "sparkSubmitParameters": (
                    "--conf spark.sql.adaptive.enabled=true "
                    "--conf spark.sql.adaptive.skewJoin.enabled=true "
                    "--conf spark.sql.sources.partitionOverwriteMode=dynamic "
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
                    "logUri": f"s3://{bucket}/spark-logs/{run.tag}/",
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
# Orchestration                                                               #
# --------------------------------------------------------------------------- #


def parse_runs(args) -> list[MonthRun]:
    """Build the list of (year, month) runs from CLI args."""
    runs: list[MonthRun] = []

    if args.years:
        years = sorted({int(y.strip()) for y in args.years.split(",") if y.strip()})
        for y in years:
            runs.extend(MonthRun(y, m) for m in range(1, 13))
    elif args.month is not None:
        runs.append(MonthRun(args.year, args.month))
    else:
        runs.extend(MonthRun(args.year, m) for m in range(1, 13))

    return runs


def submit_sequential(emr, runs: list[MonthRun], **submit_kwargs) -> int:
    """Submit one at a time, waiting between. Returns 0 if all succeed, 1 otherwise."""
    application_id = submit_kwargs["application_id"]
    failures: list[str] = []
    for run in runs:
        log.info("=== %s submit ===", run.tag)
        job_run_id = start_one_month(emr, run=run, **submit_kwargs)
        final = wait_for(emr, application_id, job_run_id, label=run.tag)
        if final["state"] != "SUCCESS":
            log.error("%s FAILED: %s", run.tag, final.get("stateDetails"))
            failures.append(run.tag)
        else:
            log.info("%s ok", run.tag)
    if failures:
        log.error("failed months: %s", ", ".join(failures))
        return 1
    return 0


def submit_parallel(emr, runs: list[MonthRun], **submit_kwargs) -> int:
    """Submit all jobs immediately, poll each in its own thread.

    The EMR app's max capacity caps actual parallelism; jobs beyond that
    queue inside the application. With our default 32 vCPU app and 4-core
    executors, ~6-8 concurrent jobs is a safe target.
    """
    application_id = submit_kwargs["application_id"]
    log.info("submitting %d jobs in parallel", len(runs))

    job_ids: dict[str, str] = {}     # tag -> job_run_id
    for run in runs:
        try:
            jid = start_one_month(emr, run=run, **submit_kwargs)
            job_ids[run.tag] = jid
            log.info("submitted %s → %s", run.tag, jid)
        except ClientError as exc:
            log.error("submit %s failed: %s", run.tag, exc)

    # Poll each job to completion in its own thread.
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=min(len(job_ids), 16)) as pool:
        futs = {
            pool.submit(wait_for, emr, application_id, jid, label=tag): tag
            for tag, jid in job_ids.items()
        }
        for fut in as_completed(futs):
            tag = futs[fut]
            final = fut.result()
            if final["state"] != "SUCCESS":
                log.error("%s FAILED: %s", tag, final.get("stateDetails"))
                failures.append(tag)

    if failures:
        log.error("failed months: %s", ", ".join(sorted(failures)))
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry: upload artifacts and submit EMR Serverless jobs (sequential or parallel)."""
    parser = argparse.ArgumentParser(description=__doc__)

    # AWS / EMR config (env-driven)
    parser.add_argument("--bucket",         default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--application-id", default=os.environ.get("EMR_APPLICATION_ID"))
    parser.add_argument("--exec-role-arn",  default=os.environ.get("EMR_EXEC_ROLE_ARN"))
    parser.add_argument("--region",         default=os.environ.get("AWS_REGION"))

    # What to run — three forms (mutually exclusive in practice):
    parser.add_argument(
        "--year", type=int,
        help="Year to process. With --month, runs that single month; without, runs all 12.",
    )
    parser.add_argument(
        "--month", type=int, choices=range(1, 13), metavar="{1..12}",
        help="Single-month run. Requires --year.",
    )
    parser.add_argument(
        "--years", type=str,
        help="Comma-separated years for full-history backfill (e.g. '2020,2021,2022').",
    )

    parser.add_argument("--parallel", action="store_true",
                        help="Submit all jobs at once (vs sequential).")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Validation
    missing = [k for k, v in {
        "bucket": args.bucket,
        "application_id": args.application_id,
        "exec_role_arn": args.exec_role_arn,
    }.items() if not v]
    if missing:
        parser.error(f"missing: {', '.join(missing)} (pass via flag or env)")
    if not args.year and not args.years:
        parser.error("specify --year [--month] or --years")
    if args.month is not None and not args.year:
        parser.error("--month requires --year")

    runs = parse_runs(args)
    log.info("planning %d run(s): %s", len(runs), [r.tag for r in runs])

    session_kwargs = {"region_name": args.region} if args.region else {}
    session = boto3.Session(**session_kwargs)
    s3  = session.client("s3")
    emr = session.client("emr-serverless")

    script_uri, zones_uri = upload_artifacts(s3, args.bucket)

    submit_kwargs = dict(
        application_id=args.application_id,
        exec_role_arn=args.exec_role_arn,
        script_uri=script_uri,
        zones_uri=zones_uri,
        bucket=args.bucket,
    )

    if args.parallel:
        return submit_parallel(emr, runs, **submit_kwargs)
    return submit_sequential(emr, runs, **submit_kwargs)


if __name__ == "__main__":
    sys.exit(main())
