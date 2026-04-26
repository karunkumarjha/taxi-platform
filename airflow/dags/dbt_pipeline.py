"""
dbt_pipeline — daily Snowflake/dbt path.

Flow:

    list_expected_months
            │
    ingest_missing_to_s3        (TLC CloudFront → S3, smart-resume)
            │
    short_circuit_if_noop       (skip downstream when nothing new AND marts already populated)
            │
    load_raw_to_snowflake       (LOADER role; COPY INTO from external stage)
            │
    dbt_build:
        dbt_deps → dbt_source_freshness → dbt_seed →
        dbt_build_staging → dbt_build_marts → swap_marts_blue_green
            │
    notify_success

Design notes:
* Snowflake creds split per task: load_raw uses the LOADER role; everything
  dbt-related uses the DBT role. Two Airflow Connections.
* dbt_build is run per-layer so a test failure in staging blocks marts.
* swap_marts only runs on dbt_build_marts success — failure means MARTS keeps
  the previous good build, dashboards uninterrupted.
* Idempotent — safe to re-run for any logical_date.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

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
SF_DBT_CONN_ID    = "snowflake_dbt"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
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
    description="TLC raw → S3 → Snowflake → dbt → swap marts",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "snowflake", "taxi"],
) as dag:

    @task(task_id="list_expected_months")
    def list_expected_months(**context) -> list[str]:
        """Compute the list of YYYY-MM tags expected to be present by logical_date,
        gated by the taxi_year / taxi_year_end Airflow Variables."""
        from include.taxi_helpers import months_up_to

        logical_date: datetime = context["logical_date"]
        year_gate = int(Variable.get("taxi_year", default_var="2023"))
        year_cap_str = Variable.get("taxi_year_end", default_var="")
        year_cap = int(year_cap_str) if year_cap_str else None
        months = months_up_to(logical_date, year_gate=year_gate, year_cap=year_cap)
        log.info("expecting %d months by %s: %s", len(months), logical_date.date(), months)
        return months

    @task(task_id="ingest_missing_to_s3")
    def ingest_missing_to_s3(months: list[str]) -> list[str]:
        """Stream any missing months TLC → S3. Idempotent (HEAD-checks each key)."""
        from ingestion.ingest_tlc import Month, ingest_missing

        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")
        parsed = [Month(int(m.split("-")[0]), int(m.split("-")[1])) for m in months]
        uploaded = ingest_missing(parsed, bucket=bucket, prefix=prefix)
        log.info("uploaded %d keys", len(uploaded))
        return uploaded

    def _should_continue(ti) -> bool:
        """Skip downstream if nothing new AND marts already populated.
        Bootstrap: if marts table doesn't exist, always continue."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        new_keys = ti.xcom_pull(task_ids="ingest_missing_to_s3") or []
        if new_keys:
            log.info("new keys present — continue")
            return True
        try:
            hook = SnowflakeHook(snowflake_conn_id=SF_DBT_CONN_ID)
            rows = hook.get_records(
                "select count(*) from ANALYTICS.MARTS.AGG_ZONE_REVENUE_MONTHLY"
            )
            row_count = int(rows[0][0]) if rows else 0
        except Exception as exc:
            log.info("marts probe failed (likely first run): %s — continuing", exc)
            return True
        proceed = row_count == 0
        log.info("marts row_count=%d → %s", row_count, "continue" if proceed else "skip")
        return proceed

    short_circuit = ShortCircuitOperator(
        task_id="short_circuit_if_noop",
        python_callable=_should_continue,
    )

    @task(task_id="load_raw_to_snowflake", retries=1)
    def load_raw_to_snowflake() -> None:
        """COPY INTO RAW.YELLOW_TRIPDATA from external S3 stage. As LOADER."""
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
        os.environ.update({
            "SNOWFLAKE_ACCOUNT":     extra.get("account", ""),
            "SNOWFLAKE_USER":        conn.login or "",
            "SNOWFLAKE_PASSWORD":    conn.password or "",
            "SNOWFLAKE_ROLE":        extra.get("role", "LOADER"),
            "SNOWFLAKE_WAREHOUSE":   extra.get("warehouse", "WH_XS"),
            "SNOWFLAKE_DATABASE":    extra.get("database", "ANALYTICS"),
            "SNOWFLAKE_RAW_SCHEMA":  extra.get("raw_schema", "RAW"),
            "SNOWFLAKE_STAGE":       extra.get("stage", "S3_TLC_STAGE"),
            "SNOWFLAKE_FILE_FORMAT": extra.get("file_format", "PARQUET_FF"),
            "SNOWFLAKE_RAW_TABLE":   extra.get("raw_table", "YELLOW_TRIPDATA"),
        })

        cfg = _cfg_from_env()
        sf = connect(cfg)
        try:
            ensure_table(sf, cfg)
            copy_from_stage(sf, cfg)
        finally:
            sf.close()

    # ---- dbt build (per-layer, fail-fast) ---------------------------------

    def _dbt_task(
        task_id: str,
        select: str,
        *,
        extra_flags: str = "",
        retries: int = 0,
    ) -> BashOperator:
        """Build a per-layer `dbt build` BashOperator with shared env + cwd."""
        return BashOperator(
            task_id=task_id,
            bash_command=(
                f"dbt build --profiles-dir {DBT_PROFILES_DIR} "
                f"--select {select} {extra_flags} --fail-fast"
            ).strip(),
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
        )
        dbt_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=(
                f"dbt source freshness --profiles-dir {DBT_PROFILES_DIR} --select source:raw"
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
        dbt_staging      = _dbt_task("dbt_build_staging",      "path:models/staging")
        dbt_intermediate = _dbt_task("dbt_build_intermediate", "path:models/intermediate")
        dbt_marts        = _dbt_task("dbt_build_marts",        "path:models/marts")

        # Blue-green flip — only if dbt_build_marts succeeds (default
        # all_success trigger rule). Test failure → swap skipped → MARTS
        # retains the previous good build.
        swap_marts = BashOperator(
            task_id="swap_marts_blue_green",
            bash_command=(
                f"dbt run-operation swap_marts --profiles-dir {DBT_PROFILES_DIR}"
            ),
            env=DBT_ENV,
            append_env=True,
            cwd=str(DBT_PROJECT_DIR),
            retries=0,
        )

        (
            dbt_deps
            >> dbt_freshness
            >> dbt_seed
            >> dbt_staging
            >> dbt_intermediate
            >> dbt_marts
            >> swap_marts
        )

    notify_success = EmptyOperator(task_id="notify_success")

    # ---- wiring -----------------------------------------------------------

    months = list_expected_months()
    uploaded = ingest_missing_to_s3(months)
    loaded = load_raw_to_snowflake()

    uploaded >> short_circuit >> loaded >> dbt_build >> notify_success
