"""
spark_pipeline — schedule-driven, one DAG run per month.

Every DAG run processes exactly ONE month, derived from `logical_date`:
    logical_date = YYYY-MM-01  →  process YYYY-MM data
The DAG fires monthly and `max_active_runs=1` keeps backfill sequential.

Flow per run:

    compute_target_month               (logical_date → year, month)
            │
    skip_if_already_processed          (short-circuit if analytics/year=Y/month=M
                                         exists AND force is not set)
            │
    ingest_one_month                   (TLC CloudFront → S3, smart-resume,
                                         long retries to absorb TLC's ~2-month
                                         publishing lag for live runs)
            │
    process_one_month                  (submit EMR Serverless job, wait,
                                         output to analytics/year=Y/month=M/)
            │
    notify_success

Scheduled runs:
    schedule="@monthly"  start_date=2009-01-01  catchup=False
    Each month, fires once for the previous month's data interval.
    For live operation, the ingest task may need many retries while we
    wait for TLC to publish — that's expected, retry_delay covers ~3 weeks.

Backfill:
    make spark-backfill START=2020-01 END=2023-12
    Internally:
      airflow dags backfill spark_pipeline \\
        --start-date 2020-01-01 --end-date 2023-12-01
    Creates one DAG run per scheduled interval in the range.
    max_active_runs=1 → runs sequentially.
    No TLC lag concern — historical data is already published.

Trigger param (UI):
    force — reprocess even if the month is already in analytics/.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

log = logging.getLogger(__name__)

REPO_ROOT = Path("/usr/local/airflow")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="spark_pipeline",
    description=(
        "TLC raw → S3 → EMR Serverless → daily zone aggregates. "
        "One DAG run per month, schedule-driven via logical_date. "
        "Backfill: `make spark-backfill START=YYYY-MM END=YYYY-MM`."
    ),
    start_date=datetime(2009, 1, 1),     # earliest TLC year — enables backfill all the way back
    schedule="@monthly",
    catchup=False,                        # don't auto-fire history; backfill is explicit
    max_active_runs=1,                    # sequential — protects EMR app + cost
    default_args=DEFAULT_ARGS,
    tags=["spark", "emr", "taxi"],
    params={
        "force": Param(
            default=False,
            type="boolean",
            title="Force reprocess",
            description=(
                "If true, reprocess this month even if analytics/year=Y/month=M "
                "already exists. Useful for fixing a known-bad month."
            ),
        ),
        "lag_months": Param(
            default=2,
            type="integer",
            minimum=0,
            maximum=12,
            title="TLC publishing lag (months)",
            description=(
                "Subtracted from logical_date to find the month of data to "
                "process. Default 2 — TLC publishes month M roughly 2 months "
                "after M ends. Backfill should pass 0 (range = data months)."
            ),
        ),
    },
) as dag:

    @task(task_id="compute_target_month")
    def compute_target_month(**context) -> dict:
        """Derive (year, month) from logical_date minus the publishing lag.

        Scheduled runs (lag_months=2): if today is May, the run fires for
        a March-data interval, but March data isn't published yet. We
        subtract 2 months → process January data → succeeds first try.

        Backfills (lag_months=0): user-specified --start-date / --end-date
        become the actual data months processed. No offset confusion.

        Manual UI triggers: user picks lag_months in the form (default 2).
        """
        params = context["params"] or {}
        lag = int(params.get("lag_months") or 0)

        logical_date: datetime = context["logical_date"]
        # Compute logical_date - lag months without dateutil.
        total = logical_date.year * 12 + (logical_date.month - 1) - lag
        target_year, target_month = divmod(total, 12)
        target_month += 1
        target = {"year": target_year, "month": target_month}

        log.info(
            "logical_date=%s  lag=%d  →  target=%04d-%02d",
            logical_date.date(), lag, target["year"], target["month"],
        )
        return target

    def _should_run(ti, **context) -> bool:
        """Short-circuit if this month's output already exists in analytics/
        AND the force param is False. Bootstrap-safe (no analytics yet → run)."""
        import boto3

        target = ti.xcom_pull(task_ids="compute_target_month")
        force = bool((context.get("params") or {}).get("force"))

        if force:
            log.info("force=True → run regardless of existing output")
            return True

        bucket = Variable.get("s3_bucket")
        prefix = (
            f"analytics/daily_zone_aggregates/"
            f"year={target['year']}/month={target['month']}/"
        )
        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        already_processed = bool(resp.get("Contents"))
        log.info("analytics/%s exists=%s", prefix, already_processed)
        return not already_processed

    skip_if_processed = ShortCircuitOperator(
        task_id="skip_if_already_processed",
        python_callable=_should_run,
    )

    @task(
        task_id="ingest_one_month",
        # The default lag_months=2 normally means we target a month TLC has
        # already published, so first-try success. Modest retries cover edge
        # cases — TLC occasionally takes 3+ months for a delayed publish, or
        # CloudFront has a transient hiccup. ~3 days of slack.
        retries=6,
        retry_delay=timedelta(hours=12),
        retry_exponential_backoff=False,
    )
    def ingest_one_month(target: dict) -> str:
        """Stream this month's TLC parquet into S3 raw/. Idempotent — HEAD
        check first, only uploads if missing. Fails (and retries) on 404
        when TLC hasn't published yet."""
        from ingestion.ingest_tlc import Month, ingest_missing

        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")
        month = Month(target["year"], target["month"])
        uploaded = ingest_missing([month], bucket=bucket, prefix=prefix)
        log.info("uploaded=%d (key already present means it was a no-op)", len(uploaded))
        return month.tag

    @task(task_id="process_one_month", retries=0)
    def process_one_month(target: dict) -> str:
        """Submit ONE EMR Serverless job for this month and wait for terminal state."""
        import os
        import sys as _sys

        from spark.submit_emr import main as submit_main

        bucket = Variable.get("s3_bucket")
        application_id = Variable.get("emr_application_id")
        exec_role_arn = Variable.get("emr_exec_role_arn")
        region = Variable.get("aws_region", default_var="us-east-1")

        os.environ.update({
            "S3_BUCKET":          bucket,
            "EMR_APPLICATION_ID": application_id,
            "EMR_EXEC_ROLE_ARN":  exec_role_arn,
            "AWS_REGION":         region,
        })

        argv = ["--year", str(target["year"]), "--month", str(target["month"])]
        rc = submit_main(argv)
        if rc != 0:
            _sys.exit(rc)
        return (
            f"s3://{bucket}/analytics/daily_zone_aggregates/"
            f"year={target['year']}/month={target['month']}/"
        )

    notify_success = EmptyOperator(task_id="notify_success")

    # ---- wiring -----------------------------------------------------------

    target = compute_target_month()
    target >> skip_if_processed
    uploaded = ingest_one_month(target)
    skip_if_processed >> uploaded
    output = process_one_month(target)
    uploaded >> output >> notify_success
