"""
spark_pipeline — historical Spark backfill, manual-trigger only.

One DAG run = one chosen year (or single month). All 12 months of the
chosen year fan out into mapped task instances via Airflow's dynamic
task mapping, so the UI shows 12 tiles for a year-long run — each
independently retriable.

Why this DAG exists:
    Spark replicates dbt's staging logic (12 validity rules + trip_sk +
    derived columns + zone enrichment) and writes split FCT_TRIPS /
    FCT_TRIPS_QUARANTINED parquet to s3://.../staged-marts/. The
    dbt_pipeline DAG's load_spark_staged_into_marts_build macro then
    COPY-INTOs those files into MARTS_BUILD on its next run.
    Spark = historical bulk staging. Live months are owned entirely by
    dbt_pipeline.

Why no schedule:
    Spark is purely historical. Live months are dbt_pipeline's job.
    schedule=None makes the manual-trigger intent explicit — there's no
    scheduled @monthly fire and no "what should the lag be?" decision.

Trigger via UI:
    1. Click spark_pipeline → ▶ Trigger DAG w/ config
    2. Set Params:
       • year   (required, 2009–2030) — process this year
       • month  (optional, 0 = all 12 months; 1–12 = single month)
       • force  (optional) — reprocess even if already in staged-marts/
    3. Trigger → see N mapped task instances in the Grid view.

Trigger via CLI (dev):
    astro dev run dags trigger spark_pipeline \\
        --conf '{"year": 2010, "force": true}'

Multi-year via UI: trigger once per year. max_active_runs=1 queues
them sequentially.

Failure isolation:
    Each mapped task is one (year, month). Per-month failure isolation
    is automatic — a failed month's task can be cleared in the UI Grid
    view to retry just that one without rerunning the year.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

# Logical dataset identifier shared with dbt_pipeline. URI is arbitrary —
# Airflow matches producer outlets and consumer schedules by exact string.
# Updated on successful spark_pipeline completion → triggers dbt_pipeline.
SPARK_STAGE_DATASET = Dataset("spark+s3://staged-marts/fct_trips")

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
        "Historical Spark backfill — manual-trigger only. "
        "Set year (and optional month) Param; mapped tasks fan out to "
        "process each month. Output: s3://.../staged-marts/."
    ),
    start_date=datetime(2009, 1, 1),
    schedule=None,  # manual-trigger only — Spark is historical-only
    catchup=False,
    max_active_runs=1,  # multi-year via N triggers serialise here
    default_args=DEFAULT_ARGS,
    tags=["spark", "emr", "taxi", "historical"],
    params={
        "year": Param(
            default=2023,
            type="integer",
            minimum=2009,
            maximum=2030,
            title="Year to process",
            description=(
                "All 12 months of this year fan out into mapped task "
                "instances. Override `month` to process a single month "
                "instead."
            ),
        ),
        "month": Param(
            default=0,
            type="integer",
            minimum=0,
            maximum=12,
            title="Month (0 = all 12 months, 1–12 = single month)",
            description=(
                "Leave at 0 for a whole-year run (12 mapped tasks). Set "
                "1–12 to process just that month (1 mapped task)."
            ),
        ),
        "force": Param(
            default=False,
            type="boolean",
            title="Force reprocess",
            description=(
                "If true, reprocess each month even when "
                "staged-marts/fct_trips/{tag}/ already exists. Useful "
                "for fixing a known-bad month."
            ),
        ),
    },
) as dag:

    @task(task_id="enumerate_months")
    def enumerate_months(**context) -> list[dict]:
        """Generate one {year, month} dict per month to process.

        Returns 12 dicts for a whole-year run, or 1 dict if month != 0.
        The list is what `process_one_month.expand(...)` consumes —
        Airflow creates one mapped task instance per element.
        """
        params = context["params"] or {}
        year = int(params["year"])
        month = int(params.get("month") or 0)

        if month == 0:
            result = [{"year": year, "month": m} for m in range(1, 13)]
            log.info("fanning out to all 12 months of %d", year)
        else:
            result = [{"year": year, "month": month}]
            log.info("processing single month %04d-%02d", year, month)
        return result

    @task(
        task_id="process_one_month",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    def process_one_month(target: dict, **context) -> str:
        """Per-month workhorse — runs once per mapped task instance.

        Three steps:
          1. Ingest the TLC parquet from CloudFront → s3://bucket/raw/
             (idempotent — S3 HEAD-checks first, only uploads if missing).
          2. Skip-if-already-processed: if staged-marts/fct_trips/{tag}/
             already exists AND force is False, return early.
          3. Submit one EMR Serverless job; wait for terminal state.
        """
        import os
        import sys as _sys

        import boto3

        from ingestion.ingest_tlc import Month, ingest_missing
        from spark.submit_emr import main as submit_main

        bucket = Variable.get("s3_bucket")
        force = bool((context.get("params") or {}).get("force"))
        tag = f"{target['year']:04d}-{target['month']:02d}"

        # ---- 1. Ingest -------------------------------------------------
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")
        month = Month(target["year"], target["month"])
        uploaded = ingest_missing([month], bucket=bucket, prefix=prefix)
        log.info(
            "ingest %s: uploaded=%d (key already present means it was a no-op)",
            tag,
            len(uploaded),
        )

        # ---- 2. Skip-if-processed (unless force) -----------------------
        if not force:
            s3 = boto3.client("s3")
            staged_prefix = f"staged-marts/fct_trips/{tag}/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=staged_prefix, MaxKeys=1)
            if resp.get("Contents"):
                log.info(
                    "%s already staged at s3://%s/%s — skipping EMR job",
                    tag,
                    bucket,
                    staged_prefix,
                )
                return f"skipped: {tag}"

        # ---- 3. Submit EMR Serverless job ------------------------------
        application_id = Variable.get("emr_application_id")
        exec_role_arn = Variable.get("emr_exec_role_arn")
        region = Variable.get("aws_region", default_var="us-east-1")
        os.environ.update(
            {
                "S3_BUCKET": bucket,
                "EMR_APPLICATION_ID": application_id,
                "EMR_EXEC_ROLE_ARN": exec_role_arn,
                "AWS_REGION": region,
            }
        )

        argv = ["--year", str(target["year"]), "--month", str(target["month"])]
        rc = submit_main(argv)
        if rc != 0:
            _sys.exit(rc)
        return f"s3://{bucket}/staged-marts/fct_trips/{tag}/"

    # outlets=[SPARK_STAGE_DATASET] fires a dataset event when this task
    # succeeds — Airflow scheduler then triggers dbt_pipeline (which has the
    # same dataset declared in its schedule). One event per spark_pipeline
    # run, after ALL mapped months have completed.
    notify_success = EmptyOperator(
        task_id="notify_success",
        outlets=[SPARK_STAGE_DATASET],
    )

    # ---- Wiring ------------------------------------------------------------

    months = enumerate_months()
    # max_active_tis_per_dag=1 forces strictly sequential execution of the
    # mapped task instances. UI still shows 12 tiles for a year-long run,
    # but only one runs at a time — cleaner log streams + predictable EMR
    # capacity usage. Trade-off vs parallel: ~50 min wall time instead of
    # ~10-15 min, accepted because:
    #   * EMR Serverless cost is identical (vCPU-hours don't change).
    #   * Free-trial accounts (Snowflake / AWS limits) prefer steady serial
    #     submission over short bursts.
    #   * Failed-month retry semantics are unchanged — clear the failed
    #     mapped task, it re-runs on its own.
    results = process_one_month.partial(
        max_active_tis_per_dag=1,
    ).expand(target=months)
    results >> notify_success
