"""
dbt_pipeline — per-month incremental dbt build with SCD snapshot. One DagRun = one month.

Trigger sources:
  • @monthly cron (live)
  • `airflow dags backfill` / `make dbt-backfill` (historical, sequential via max_active_runs=1)
  • Manual UI trigger with target_year/target_month in conf (re-run a specific month)

Per-run flow:
    compute_target_month  →  detect_source_drift  →  partition_replace_if_drifted
        →  ingest_one_month  →  load_one_month_to_snowflake
        →  dbt_build (deps → freshness → reset_marts_build → snapshot → seed
                      → staging → intermediate → marts → swap_marts)
        →  record_fingerprint   (post-build only — failed run leaves stored ETag for retry)

target_month resolution (compute_target_month):
  1. dag_run.conf has target_year + target_month → use directly.
  2. run_type='backfill' → use data_interval_start (literal data month from --start-date).
  3. Else → today − lag_months. Anchored on today() because Airflow 3.x's logical_date
     equals run_after (end of interval), which puts us off TLC's real publishing cadence.
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
    """Failure alert via direct smtplib.

    Sidesteps Airflow 3.0.x's built-in email_on_failure, whose Jinja
    template crashes on `ti.mark_success_url` (doesn't exist on the new
    RuntimeTaskInstance). Silent no-op when ALERT_EMAIL unset; SMTP
    exceptions are swallowed so a bad mailbox doesn't break task failure handling.
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
        """Derive (year, month) for this run, in priority order:
          1. dag_run.conf['target_year' + 'target_month']  — manual UI re-run
          2. run_type='backfill'                          — data_interval_start
          3. scheduled / manual                            — today − lag_months

        Anchored on today() (not logical_date) for scheduled runs because
        Airflow 3.x's logical_date == run_after, which trails the actual
        TLC publishing cadence. Refuses targets newer than today−2 months.
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

    @task(task_id="detect_source_drift")
    def detect_source_drift(target: dict, **context) -> dict:
        """HEAD CloudFront ETag, compare vs RAW.SOURCE_FINGERPRINTS.

        Returns {"status": "new" | "unchanged" | "drifted", "filename": ...}.
        Fingerprint is upserted later, post-build, by record_fingerprint —
        so a failed run leaves it untouched and the next run retries.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from ingestion.detect_drift import detect_drift

        ym_tag = f"{target['year']:04d}-{target['month']:02d}"
        filename = f"yellow_tripdata_{ym_tag}.parquet"

        hook = SnowflakeHook(snowflake_conn_id=SF_LOADER_CONN_ID)
        sf = hook.get_conn()
        try:
            raw_schema = (hook.get_connection(SF_LOADER_CONN_ID).extra_dejson or {}).get(
                "raw_schema", "RAW"
            )
            status, fp = detect_drift(sf, filename, raw_schema=raw_schema)
        finally:
            sf.close()

        log.info("drift %s: %s (etag=%s)", filename, status, fp.etag if fp else "n/a")
        return {"status": status, "filename": filename}

    @task(task_id="partition_replace_if_drifted", trigger_rule="none_failed")
    def partition_replace_if_drifted(drift: dict, **context) -> dict:
        """No-op unless drifted. On drift, wipe raw + snapshot rows for the file."""
        if drift["status"] != "drifted":
            return drift

        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from ingestion.detect_drift import partition_replace

        # DBT role has DELETE on raw + snapshots; LOADER does not.
        hook = SnowflakeHook(snowflake_conn_id=SF_DBT_CONN_ID)
        sf = hook.get_conn()
        try:
            db = (hook.get_connection(SF_DBT_CONN_ID).extra_dejson or {}).get(
                "database", "ANALYTICS"
            )
            partition_replace(
                sf,
                drift["filename"],
                raw_table=f"{db}.RAW.YELLOW_TRIPDATA",
                snapshot_table=f"{db}.SNAPSHOTS.SNP_YELLOW_TRIPS",
            )
            sf.commit()
        finally:
            sf.close()
        return drift

    @task(
        task_id="ingest_one_month",
        retries=6,
        retry_delay=timedelta(hours=12),
        retry_exponential_backoff=False,
    )
    def ingest_one_month(target: dict, drift: dict, **context) -> str:
        """CloudFront → S3 raw/. force=True if drift detected."""
        from ingestion.ingest_tlc import Month, ingest_missing

        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")
        month = Month(target["year"], target["month"])
        force = drift.get("status") == "drifted"
        uploaded = ingest_missing([month], bucket=bucket, prefix=prefix, force=force)
        log.info(
            "uploaded=%d force=%s (0 + force=False = file was already present)",
            len(uploaded),
            force,
        )
        return month.tag

    @task(task_id="load_one_month_to_snowflake", retries=1)
    def load_one_month_to_snowflake(target: dict, **context) -> str:
        """COPY INTO RAW.YELLOW_TRIPDATA, FORCE=TRUE (append-only). Snapshot dedupes downstream."""
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
        """Per-layer `dbt build`. Passes target vars so incremental models scope correctly."""
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
        # SCD2 snapshot. Writes to SNAPSHOTS schema, not MARTS_BUILD,
        # so it persists across the blue-green swap.
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

    @task(task_id="record_fingerprint")
    def record_fingerprint(drift: dict, **context) -> None:
        """Persist new ETag post-build only. Failure leaves stored value untouched for retry."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from ingestion.detect_drift import fetch_tlc_fingerprint, upsert_fingerprint

        filename = drift["filename"]
        fp = fetch_tlc_fingerprint(filename)
        if fp is None:
            log.warning("no current fingerprint for %s — skipping record", filename)
            return

        hook = SnowflakeHook(snowflake_conn_id=SF_LOADER_CONN_ID)
        sf = hook.get_conn()
        try:
            raw_schema = (hook.get_connection(SF_LOADER_CONN_ID).extra_dejson or {}).get(
                "raw_schema", "RAW"
            )
            upsert_fingerprint(sf, fp, raw_schema=raw_schema)
            sf.commit()
        finally:
            sf.close()
        log.info("recorded %s: etag=%s", filename, fp.etag)

    # ---- wiring -----------------------------------------------------------

    target = compute_target_month()
    drift = detect_source_drift(target)
    replaced = partition_replace_if_drifted(drift)
    uploaded = ingest_one_month(target, drift)
    loaded = load_one_month_to_snowflake(target)
    fingerprint_recorded = record_fingerprint(drift)

    target >> drift >> replaced >> uploaded >> loaded >> dbt_build >> fingerprint_recorded
