"""
dbt_pipeline — dual-triggered, self-healing incremental dbt build.

Two trigger sources:
  • @monthly cron — live fire; ingests the target month's TLC parquet and
    runs the dbt build.
  • Airflow Dataset (SPARK_STAGE_DATASET) — fires whenever spark_pipeline
    completes; skips live TLC ingest (Spark already produced the data) and
    runs the dbt build, which COPYs the freshly-staged parquet from S3 into
    MARTS_BUILD via load_spark_staged_into_marts_build.

The dbt build itself is identical across both trigger types — count-divergence
detection picks up whatever's new in RAW + MARTS_BUILD without per-month vars.

    logical_date = YYYY-MM-01  →  ingest data for (logical_date - lag_months)
    @monthly schedule + max_active_runs=1 → sequential live + backfill

Flow per run:

    compute_target_month               (logical_date − lag → year, month)
            │
    ingest_one_month                   (TLC CloudFront → S3 raw/, smart-resume,
                                         long retries to absorb TLC's ~2-month
                                         publishing lag for live runs)
            │
    load_one_month_to_snowflake        (COPY INTO RAW.YELLOW_TRIPDATA scoped to
                                         that file's parquet)
            │
    dbt_build (TaskGroup):
        dbt_deps
          → dbt_source_freshness                (warn-only: did RAW get fresh data?)
          → reset_marts_build_from_marts        (clone MARTS → MARTS_BUILD)
          → load_spark_staged_into_marts_build  (COPY @S3_SPARK_STAGE → MARTS_BUILD;
                                                  no-op when no Spark batches staged)
          → dbt_seed
          → dbt_build_staging / intermediate / marts
                                         (each model self-detects what to rebuild
                                          via count-divergence — see model SQL)
          → swap_marts_blue_green
            │
    notify_success

Self-healing detection (no params needed):
    Each incremental model's source CTE compares fct row counts (from the
    upstream table) against this table's contents. Months whose counts
    diverge get rebuilt; months that match are skipped. A row-count threshold
    (var:phantom_month_threshold, default 100k) prevents partial-month builds
    from TLC's tiny cross-month tail-bleed.

    Net effect: ONE dbt build absorbs whatever's new — live month, historical
    Spark backfill, late-arriving leak rows merging into existing months.

Scheduled runs:
    schedule="@monthly" start_date=2009-01-01 catchup=False
    Each month, fires once for the previous month's data interval. With the
    default lag_months=2, ingest targets a month TLC has already published.

Backfill:
    make dbt-backfill START=2023-01 END=2023-12
    Internally:
      airflow dags backfill dbt_pipeline --start-date 2023-01-01 --end-date 2023-12-01
    DAG sees run_type=backfill → auto-applies lag_months=0, so START/END ARE
    the data months. max_active_runs=1 keeps runs sequential.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup

# Must match the URI that spark_pipeline declares as outlet. Producing this
# dataset (spark_pipeline → notify_success) triggers a dbt_pipeline run.
SPARK_STAGE_DATASET = Dataset("spark+s3://staged-marts/fct_trips")

log = logging.getLogger(__name__)

REPO_ROOT = Path("/usr/local/airflow")
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Make repo importable so DAG tasks can `from ingestion import ...` etc.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Two Snowflake connections — one per functional role. Aligns with the RBAC
# model: LOADER writes RAW only; DBT owns STAGING/MARTS_BUILD/MARTS.
SF_LOADER_CONN_ID = "snowflake_loader"
SF_DBT_CONN_ID = "snowflake_dbt"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Env-var template resolved at task-execution time. The BashOperator passes
# this verbatim to dbt's subprocess, where profiles.yml's env_var() lookups
# pick them up.
_DBT_CONN = "conn." + SF_DBT_CONN_ID
DBT_ENV = {
    "SNOWFLAKE_ACCOUNT": "{{ " + _DBT_CONN + ".extra_dejson.account }}",
    "SNOWFLAKE_USER": "{{ " + _DBT_CONN + ".login }}",
    "SNOWFLAKE_PASSWORD": "{{ " + _DBT_CONN + ".password }}",
    "SNOWFLAKE_ROLE": "{{ " + _DBT_CONN + ".extra_dejson.get('role', 'DBT') }}",
    "SNOWFLAKE_WAREHOUSE": "{{ " + _DBT_CONN + ".extra_dejson.get('warehouse', 'WH_XS') }}",
    "SNOWFLAKE_DATABASE": "{{ " + _DBT_CONN + ".extra_dejson.get('database', 'ANALYTICS') }}",
    "SNOWFLAKE_DBT_SCHEMA": "{{ " + _DBT_CONN + ".schema or 'MARTS_BUILD' }}",
    "SNOWFLAKE_RAW_SCHEMA": "RAW",
}


with DAG(
    dag_id="dbt_pipeline",
    description=(
        "TLC raw → S3 → Snowflake → dbt (self-healing incremental) → swap marts. "
        "One DAG run per month. Backfill: `make dbt-backfill START=YYYY-MM END=YYYY-MM`."
    ),
    start_date=datetime(2009, 1, 1),  # earliest TLC year — enables backfill all the way back
    # Two trigger sources, OR-ed:
    #   1. @monthly cron — live monthly fire (ingest TLC + run dbt build).
    #   2. SPARK_STAGE_DATASET — fires whenever spark_pipeline completes.
    # Dataset-triggered runs skip ingest_one_month / load_one_month_to_snowflake
    # (Spark already produced the data); load_spark_staged_into_marts_build
    # COPYs the new staged parquet into MARTS_BUILD as part of the dbt build.
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("@monthly", timezone="UTC"),
        datasets=[SPARK_STAGE_DATASET],
    ),
    catchup=False,  # don't auto-fire history; backfill is explicit
    max_active_runs=1,  # sequential — keeps live + backfill ordered
    default_args=DEFAULT_ARGS,
    tags=["dbt", "snowflake", "taxi"],
    params={
        "lag_months": Param(
            default=2,
            type="integer",
            minimum=0,
            maximum=12,
            title="TLC publishing lag (months) — explicit override",
            description=(
                "Subtracted from logical_date to find the month of data to "
                "ingest. Auto-defaults: 2 for scheduled/manual runs (TLC's "
                "publishing lag), 0 for backfill runs (start/end ARE the "
                "data months). Override here only if you need to deviate — "
                "the form-default value of 2 is ignored unless you also "
                "pass it explicitly via dag_run.conf."
            ),
        ),
    },
) as dag:

    @task(task_id="compute_target_month")
    def compute_target_month(**context) -> dict:
        """Derive (year, month) for ingest from logical_date minus the publishing lag.

        Lag is auto-inferred from dag_run.run_type:
          • scheduled / manual → lag=2 (TLC publishes ~2 months after M ends,
            so logical_date − 2 = the latest published month).
          • backfill           → lag=0 (--start-date / --end-date ARE the
            actual data months — no offset).

        Explicit override via `dag_run.conf` (read from raw conf, NOT from
        params, because Airflow merges Param defaults into params and we
        can't tell "user passed 2" from "user took the default").

        Note: this only affects which TLC parquet to ingest. The dbt build
        itself is data-driven (count-divergence) and doesn't need the
        target month at all.
        """
        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        if "lag_months" in conf:
            lag = int(conf["lag_months"])
            source = "conf override"
        else:
            lag = 0 if dag_run.run_type == "backfill" else 2
            source = f"auto (run_type={dag_run.run_type})"

        logical_date: datetime = context["logical_date"]
        # Compute logical_date - lag months without dateutil.
        total = logical_date.year * 12 + (logical_date.month - 1) - lag
        target_year, target_month = divmod(total, 12)
        target_month += 1
        target = {"year": target_year, "month": target_month}

        log.info(
            "logical_date=%s  lag=%d (%s)  →  ingest target=%04d-%02d",
            logical_date.date(),
            lag,
            source,
            target["year"],
            target["month"],
        )
        return target

    @task(
        task_id="ingest_one_month",
        # The default lag_months=2 normally targets a month TLC has already
        # published, so first-try success. Modest retries cover edge cases —
        # TLC occasionally takes 3+ months for a delayed publish, or
        # CloudFront has a transient hiccup. ~3 days of slack.
        retries=6,
        retry_delay=timedelta(hours=12),
        retry_exponential_backoff=False,
    )
    def ingest_one_month(target: dict, **context) -> str:
        """Stream this month's TLC parquet into S3 raw/. Idempotent — HEAD
        check first, only uploads if missing. Fails (and retries) on 404
        when TLC hasn't published yet.

        Skipped on dataset-triggered runs: spark_pipeline already wrote the
        historical data to S3 staged-marts/, and dbt's load_spark_staged
        macro will pull it into MARTS_BUILD downstream. No live TLC needed.
        """
        if context["dag_run"].run_type == "dataset_triggered":
            raise AirflowSkipException(
                "dataset-triggered run — Spark stage covers the data; skipping live TLC ingest."
            )

        from ingestion.ingest_tlc import Month, ingest_missing

        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")
        month = Month(target["year"], target["month"])
        uploaded = ingest_missing([month], bucket=bucket, prefix=prefix)
        log.info("uploaded=%d (key already present means it was a no-op)", len(uploaded))
        return month.tag

    @task(task_id="load_one_month_to_snowflake", retries=1)
    def load_one_month_to_snowflake(target: dict, **context) -> str:
        """COPY INTO RAW.YELLOW_TRIPDATA scoped to one file (target month).
        Snowflake's COPY history makes re-runs no-ops. As LOADER role.

        Skipped on dataset-triggered runs — see ingest_one_month docstring.
        """
        if context["dag_run"].run_type == "dataset_triggered":
            raise AirflowSkipException(
                "dataset-triggered run — no live TLC to load; "
                "Spark output is COPY'd via load_spark_staged_into_marts_build."
            )

        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from ingestion.load_snowflake import (
            _cfg_from_env,
            connect,
            copy_from_stage,
            ensure_table,
        )

        # Pipe LOADER conn → env vars for the shared loader module.
        hook = SnowflakeHook(snowflake_conn_id=SF_LOADER_CONN_ID)
        conn = hook.get_connection(SF_LOADER_CONN_ID)
        extra = conn.extra_dejson or {}
        os.environ.update(
            {
                "SNOWFLAKE_ACCOUNT": extra.get("account", ""),
                "SNOWFLAKE_USER": conn.login or "",
                "SNOWFLAKE_PASSWORD": conn.password or "",
                "SNOWFLAKE_ROLE": extra.get("role", "LOADER"),
                "SNOWFLAKE_WAREHOUSE": extra.get("warehouse", "WH_XS"),
                "SNOWFLAKE_DATABASE": extra.get("database", "ANALYTICS"),
                "SNOWFLAKE_RAW_SCHEMA": extra.get("raw_schema", "RAW"),
                "SNOWFLAKE_STAGE": extra.get("stage", "S3_TLC_STAGE"),
                "SNOWFLAKE_FILE_FORMAT": extra.get("file_format", "PARQUET_FF"),
                "SNOWFLAKE_RAW_TABLE": extra.get("raw_table", "YELLOW_TRIPDATA"),
            }
        )

        ym_tag = f"{target['year']:04d}-{target['month']:02d}"
        cfg = _cfg_from_env()
        sf = connect(cfg)
        try:
            ensure_table(sf, cfg)
            copy_from_stage(sf, cfg, month=ym_tag)
        finally:
            sf.close()
        return ym_tag

    # ---- dbt build (per-layer, fail-fast, fully self-detecting) -----------

    def _dbt_build_layer(task_id: str, select: str, *, retries: int = 0) -> BashOperator:
        """Build a per-layer `dbt build` BashOperator with shared env + cwd.

        No --vars passed: every model self-detects what to rebuild from data
        state via count-divergence (see model SQL).
        """
        return BashOperator(
            task_id=task_id,
            bash_command=(
                f"dbt build --profiles-dir {DBT_PROFILES_DIR} --select {select} --fail-fast"
            ),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=retries,
        )

    with TaskGroup(group_id="dbt_build") as dbt_build:
        # trigger_rule="none_failed" — must run on dataset-triggered runs
        # too, where ingest_one_month / load_one_month_to_snowflake skipped.
        # Default all_success would propagate the skip into the dbt build.
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"dbt deps --profiles-dir {DBT_PROFILES_DIR}",
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            trigger_rule="none_failed",
        )
        # Source freshness — emits warnings (not errors) on stale RAW data.
        # Configured in dbt/models/staging/sources.yml. Production-grade signal
        # without making the DAG fail when freshness slips, since count-divergence
        # detection downstream handles the actual "did we have new data" question.
        # `--no-warn-error` keeps the run going if freshness windows are exceeded.
        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=(
                f"dbt source freshness --profiles-dir {DBT_PROFILES_DIR} "
                f"--select source:raw || true"
            ),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=0,
        )
        # Clone MARTS → MARTS_BUILD so incremental models build against current
        # production state. See dbt/macros/reset_marts_build_from_marts.sql.
        reset_marts_build = BashOperator(
            task_id="reset_marts_build_from_marts",
            bash_command=(
                f"dbt run-operation reset_marts_build_from_marts --profiles-dir {DBT_PROFILES_DIR}"
            ),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=0,
        )
        # Pull any Spark-staged historical FCT_TRIPS / FCT_TRIPS_QUARANTINED
        # batches into MARTS_BUILD via COPY @S3_SPARK_STAGE. Idempotent via
        # Snowflake's COPY load history (filename-based, 64-day TTL). No-op
        # when no new staged files are present. See
        # dbt/macros/load_spark_staged_into_marts_build.sql.
        load_spark_staged = BashOperator(
            task_id="load_spark_staged_into_marts_build",
            bash_command=(
                f"dbt run-operation load_spark_staged_into_marts_build "
                f"--profiles-dir {DBT_PROFILES_DIR}"
            ),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=0,
        )
        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"dbt seed --profiles-dir {DBT_PROFILES_DIR}",
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
        )
        dbt_staging = _dbt_build_layer("dbt_build_staging", "path:models/staging")
        dbt_intermediate = _dbt_build_layer("dbt_build_intermediate", "path:models/intermediate")
        dbt_marts = _dbt_build_layer("dbt_build_marts", "path:models/marts")

        # Blue-green flip — only fires if dbt_build_marts succeeds (default
        # all_success trigger rule). Test failure → swap skipped → MARTS
        # retains the previous good build, MARTS_BUILD has the polluted
        # partial state. The next run's reset_marts_build wipes it cleanly.
        swap_marts = BashOperator(
            task_id="swap_marts_blue_green",
            bash_command=(f"dbt run-operation swap_marts --profiles-dir {DBT_PROFILES_DIR}"),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=0,
        )

        (
            dbt_deps
            >> dbt_source_freshness
            >> reset_marts_build
            >> load_spark_staged
            >> dbt_seed
            >> dbt_staging
            >> dbt_intermediate
            >> dbt_marts
            >> swap_marts
        )

    notify_success = EmptyOperator(task_id="notify_success")

    # ---- wiring -----------------------------------------------------------

    target = compute_target_month()
    uploaded = ingest_one_month(target)
    loaded = load_one_month_to_snowflake(target)
    target >> uploaded >> loaded >> dbt_build >> notify_success
