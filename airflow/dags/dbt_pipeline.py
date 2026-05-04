"""
dbt_pipeline — per-month incremental dbt build with SCD snapshot.

The single Airflow DAG for the platform. One DagRun = one month.

Trigger sources:
  • @monthly cron — live fire; ingests the target month's TLC parquet
    and runs the full dbt build (snapshot + staging + intermediate +
    marts + blue-green swap).
  • Operator-driven backfill — `airflow dags backfill dbt_pipeline
    --start-date YYYY-MM-DD --end-date YYYY-MM-DD`, wrapped by
    `make dbt-backfill`. Same DAG, max_active_runs=1 serializes the
    range into one DagRun per month. The DAG auto-detects
    run_type=backfill and uses lag_months=0 so START/END are literal
    data months.
  • Manual UI trigger — for re-running a specific month after a fix.
    Pass `target_year` + `target_month` in conf to override the auto
    target-month computation.

Target month resolution (compute_target_month):
  1. dag_run.conf has target_year + target_month → use them directly.
  2. run_type='backfill' → use data_interval_start (the literal data
     month the operator passed via --start-date / --end-date).
  3. Otherwise (scheduled / manual) → today's calendar date − lag_months.
     Anchored on today() rather than logical_date because in Airflow 3.x
     logical_date == run_after (end of interval), and even in 2.x for
     @monthly + catchup=False it sits at the start of the previous data
     interval — both put us off the real-world TLC publishing cadence.

dbt build flow per run:

    compute_target_month               (year + month for this run)
            │
    ingest_one_month                   (TLC CloudFront → S3 raw/;
                                         HEAD-skip idempotent)
            │
    load_one_month_to_snowflake        (COPY INTO RAW.YELLOW_TRIPDATA;
                                         FORCE=TRUE, audit columns)
            │
    dbt_build (TaskGroup):
        dbt_deps
          → dbt_source_freshness
          → reset_marts_build_from_marts   (clone MARTS → MARTS_BUILD)
          → dbt_snapshot                   (SCD Type 2: snp_yellow_trips)
          → dbt_seed
          → dbt_build_staging              (reads from snapshot; is a view)
          → dbt_build_intermediate         (merge on trip_bk, month-scoped)
          → dbt_build_marts                (aggregates from fct_trips)
          → swap_marts_blue_green

Bronze/Silver/Gold medallion layers:
  Bronze — RAW.YELLOW_TRIPDATA (append-only, FORCE=TRUE, _ingest_batch_id)
  Silver — SNAPSHOTS.snp_yellow_trips (SCD Type 2, trip_bk unique key)
  Gold   — MARTS.FCT_TRIPS + aggregates (valid trips, zone-enriched,
             merged on trip_bk so TLC corrections overwrite stale rows)

Scheduled runs:
    schedule="@monthly" start_date=2009-01-01 catchup=False

Backfill:
    airflow dags backfill dbt_pipeline --start-date 2023-01-01 --end-date 2023-12-01
    Set lag_months=0 so start/end ARE the data months. max_active_runs=1
    keeps runs sequential.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.definitions.param import Param
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

REPO_ROOT = Path("/usr/local/airflow")
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SF_LOADER_CONN_ID = "snowflake_loader"
SF_DBT_CONN_ID = "snowflake_dbt"

# Failure-alert recipient comes from the ALERT_EMAIL env var rendered into
# airflow/.env by bootstrap. Empty string → alerting silently disabled
# (no crash, no email) so the DAG still runs in environments without SMTP.
_ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")


def _send_failure_email(context: dict) -> None:
    """Send a failure-alert email via direct smtplib.

    Why not Airflow's built-in email_on_failure? Airflow 3.0.x's default
    failure-email Jinja template references `ti.mark_success_url`, which
    doesn't exist on the new RuntimeTaskInstance — the email send crashes
    with a Jinja UndefinedError and no alert goes out. This callback
    sidesteps the template entirely, reading SMTP config from env vars
    rendered into airflow/.env by bootstrap.

    Idempotent + safe: silently no-ops if ALERT_EMAIL is unset, and
    catches all SMTP exceptions so a misconfigured mailbox doesn't bring
    down the actual task's failure handling.
    """
    import smtplib
    from email.mime.text import MIMEText

    if not _ALERT_EMAIL:
        return

    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        return

    body = (
        f"Task {ti.dag_id}.{ti.task_id} FAILED.\n\n"
        f"Run ID:        {getattr(ti, 'run_id', 'unknown')}\n"
        f"Try number:    {getattr(ti, 'try_number', '?')}\n"
        f"Logical date:  {context.get('logical_date', '?')}\n\n"
        f"Check the Airflow UI Grid view for the full task log."
    )
    msg = MIMEText(body)
    msg["Subject"] = f"[Airflow FAILED] {ti.dag_id}.{ti.task_id}"
    msg["From"] = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM", _ALERT_EMAIL)
    msg["To"] = _ALERT_EMAIL

    try:
        host = os.environ["AIRFLOW__SMTP__SMTP_HOST"]
        port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "587"))
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if os.environ.get("AIRFLOW__SMTP__SMTP_STARTTLS", "True").lower() == "true":
                smtp.starttls()
            user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
            pwd = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")
            if user and pwd:
                smtp.login(user, pwd)
            smtp.send_message(msg)
    except Exception as e:  # noqa: BLE001 — last-ditch alert; don't propagate
        log.warning("failure-email send failed: %s", e)


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    # Airflow 3.0.x's built-in email_on_failure is broken (template bug);
    # use on_failure_callback with smtplib directly. Empty list when
    # ALERT_EMAIL is unset → alerting silently disabled.
    "on_failure_callback": [_send_failure_email] if _ALERT_EMAIL else [],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_DBT_CONN = "conn." + SF_DBT_CONN_ID

# Target year/month are set per run (from conf or logical_date − lag).
# BashOperators pick them up via env-var substitution in --vars.
DBT_ENV = {
    "SNOWFLAKE_ACCOUNT": "{{ " + _DBT_CONN + ".extra_dejson.account }}",
    "SNOWFLAKE_USER": "{{ " + _DBT_CONN + ".login }}",
    "SNOWFLAKE_PASSWORD": "{{ " + _DBT_CONN + ".password }}",
    "SNOWFLAKE_ROLE": "{{ " + _DBT_CONN + ".extra_dejson.get('role', 'DBT') }}",
    "SNOWFLAKE_WAREHOUSE": "{{ " + _DBT_CONN + ".extra_dejson.get('warehouse', 'WH_XS') }}",
    "SNOWFLAKE_DATABASE": "{{ " + _DBT_CONN + ".extra_dejson.get('database', 'ANALYTICS') }}",
    "SNOWFLAKE_DBT_SCHEMA": "{{ " + _DBT_CONN + ".schema or 'MARTS_BUILD' }}",
    "SNOWFLAKE_RAW_SCHEMA": "RAW",
    "SNOWFLAKE_SNAPSHOTS_SCHEMA": "SNAPSHOTS",
    # XCom-pulled at render time — set by compute_target_month.
    "DBT_TARGET_YEAR": "{{ task_instance.xcom_pull(task_ids='compute_target_month')['year'] }}",
    "DBT_TARGET_MONTH": "{{ task_instance.xcom_pull(task_ids='compute_target_month')['month'] }}",
}


def _enforce_publishing_lag(target: dict) -> None:
    """Raise AirflowException if `target` is newer than today − 2 months.

    Hard cap on how recent we'll process. TLC's ~2-month publishing lag
    means anything more recent isn't on the CloudFront URL yet; trying to
    ingest it would retry-loop and eventually fail with a 404. Failing
    immediately makes the operator error obvious.
    """
    today = datetime.now(UTC).date()
    target_idx = target["year"] * 12 + (target["month"] - 1)
    cap_idx = today.year * 12 + (today.month - 1) - 2
    if target_idx > cap_idx:
        cap_year, cap_month0 = divmod(cap_idx, 12)
        raise AirflowException(
            f"Target {target['year']:04d}-{target['month']:02d} is newer "
            f"than the publishing-lag cap ({cap_year:04d}-{cap_month0 + 1:02d}). "
            "TLC publishes ~2 months in arrears — pick a target ≤ "
            f"{cap_year:04d}-{cap_month0 + 1:02d}, or wait until TLC publishes."
        )


with DAG(
    dag_id="dbt_pipeline",
    description=(
        "TLC raw → S3 → Snowflake → dbt (SCD snapshot + incremental merge) → swap marts. "
        "One DAG run per month. Spark-triggered runs skip ingest/load (already done)."
    ),
    start_date=datetime(2009, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
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
                "ingest. Ignored when target_year/target_month are in conf. "
                "Auto-defaults: 2 for scheduled/manual runs, 0 for backfill."
            ),
        ),
    },
) as dag:

    @task(task_id="compute_target_month")
    def compute_target_month(**context) -> dict:
        """Derive (year, month) for this run.

        Priority:
          1. dag_run.conf['target_year'] + conf['target_month'] — set by
             a manual UI trigger that wants to re-run a specific month.
          2. Backfill runs (run_type='backfill') — use logical_date
             directly, since `--start-date YYYY-MM-01 --end-date YYYY-MM-01`
             means the operator wants those literal data months processed.
          3. Scheduled / manual runs — use TODAY's calendar date minus
             lag_months. This intentionally ignores Airflow's logical_date
             because for `@monthly` schedules logical_date is the START of
             the previous data interval, which would put us one month
             behind what TLC has actually published. We want "the most
             recent month TLC publishes" which tracks the real-world
             calendar, not the schedule semantics.

        Refuses any target newer than today − 2 months (the publishing-lag
        cap). TLC publishes ~2 months in arrears, so anything more recent
        isn't published yet — ingest_one_month would just retry-loop until
        it gave up. Failing fast here surfaces the bad input clearly.
        """
        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        if "target_year" in conf and "target_month" in conf:
            target = {"year": int(conf["target_year"]), "month": int(conf["target_month"])}
            log.info(
                "using conf-provided target: %04d-%02d",
                target["year"],
                target["month"],
            )
            _enforce_publishing_lag(target)
            return target

        is_backfill = dag_run.run_type == "backfill"

        if is_backfill:
            # Backfill: use data_interval_start as the literal data month.
            # The operator's --start-date / --end-date defines the range
            # explicitly; lag adjustments would surprise them.
            #
            # We use data_interval_start (not logical_date) because the
            # semantics of `logical_date` changed between Airflow 2.x and
            # 3.x — in 2.x it equalled data_interval_start, in 3.x it
            # equals run_after (end of the interval). data_interval_start
            # is stable across both versions.
            di_start: datetime = context["data_interval_start"]
            target = {"year": di_start.year, "month": di_start.month}
            log.info(
                "backfill — data_interval_start=%s → ingest target=%04d-%02d",
                di_start.date(),
                target["year"],
                target["month"],
            )
            _enforce_publishing_lag(target)
            return target

        # Scheduled / manual: anchor on TODAY, not logical_date. For
        # @monthly + catchup=False, logical_date sits at the start of the
        # previous data interval, which would be one month behind what
        # TLC has actually published. Today's calendar date matches the
        # real-world publishing cadence.
        if "lag_months" in conf:
            lag = int(conf["lag_months"])
            source = "conf override"
        else:
            lag = 2
            source = f"auto (run_type={dag_run.run_type})"

        today = datetime.now(UTC).date()
        total = today.year * 12 + (today.month - 1) - lag
        target_year, target_month = divmod(total, 12)
        target_month += 1
        target = {"year": target_year, "month": target_month}

        log.info(
            "today=%s  lag=%d (%s)  →  ingest target=%04d-%02d",
            today,
            lag,
            source,
            target["year"],
            target["month"],
        )
        _enforce_publishing_lag(target)
        return target

    @task(
        task_id="ingest_one_month",
        retries=6,
        retry_delay=timedelta(hours=12),
        retry_exponential_backoff=False,
    )
    def ingest_one_month(target: dict, **context) -> str:
        """Stream this month's TLC parquet into S3 raw/. Idempotent (HEAD check)."""
        from ingestion.ingest_tlc import Month, ingest_missing

        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")
        month = Month(target["year"], target["month"])
        uploaded = ingest_missing([month], bucket=bucket, prefix=prefix)
        log.info("uploaded=%d (0 = file was already present)", len(uploaded))
        return month.tag

    @task(task_id="load_one_month_to_snowflake", retries=1)
    def load_one_month_to_snowflake(target: dict, **context) -> str:
        """COPY INTO RAW.YELLOW_TRIPDATA scoped to one file. FORCE=TRUE
        makes RAW append-only — every load event is a new row set with
        fresh `_ingest_batch_id` and `_loaded_by`. The snapshot dedupes
        downstream so MARTS stays canonical.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from ingestion.load_snowflake import (
            _cfg_from_env,
            connect,
            copy_from_stage,
            ensure_table,
        )

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
            copy_from_stage(sf, cfg, month=ym_tag, loaded_by="dbt_pipeline")
        finally:
            sf.close()
        return ym_tag

    # ---- dbt build ---------------------------------------------------------

    def _dbt_build_layer(task_id: str, select: str, *, retries: int = 0) -> BashOperator:
        """Build a per-layer `dbt build` BashOperator.

        Passes target_year / target_month as dbt vars so incremental models
        (int_trips_enriched, int_trips_quarantined) scope their merge to the
        single month being processed in this DAG run.
        """
        return BashOperator(
            task_id=task_id,
            bash_command=(
                f"dbt build --profiles-dir {DBT_PROFILES_DIR} --select {select} --fail-fast"
                ' --vars "{\\"target_year\\": $DBT_TARGET_YEAR,'
                ' \\"target_month\\": $DBT_TARGET_MONTH}"'
            ),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=retries,
        )

    with TaskGroup(group_id="dbt_build") as dbt_build:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"dbt deps --profiles-dir {DBT_PROFILES_DIR}",
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            trigger_rule="none_failed",
        )
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
        # SCD Type 2 snapshot — runs over ALL of RAW each time, appending
        # new versions for corrected rows (TLC corrections) and inserting
        # new rows for newly loaded months. Writes to SNAPSHOTS schema
        # directly (not MARTS_BUILD), so it persists across blue-green swaps.
        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=f"dbt snapshot --profiles-dir {DBT_PROFILES_DIR}",
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=1,
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
            >> dbt_snapshot
            >> dbt_seed
            >> dbt_staging
            >> dbt_intermediate
            >> dbt_marts
            >> swap_marts
        )

    # ---- wiring -----------------------------------------------------------

    target = compute_target_month()
    uploaded = ingest_one_month(target)
    loaded = load_one_month_to_snowflake(target)
    target >> uploaded >> loaded >> dbt_build
