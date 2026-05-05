"""
spark_historical — manual-trigger DAG. Submits process_historical.py to
EMR Serverless, which writes THREE Iceberg tables registered in AWS Glue.
Snowflake reads zero-copy via CATALOG INTEGRATION.

Per-run shape (one year of data, configurable via the `year` Param):

  1. ensure_year_in_s3       — pre-ingest 12 parquet files from TLC
                               CloudFront → S3 raw/. Idempotent (HEAD-skip).
  2. submit_emr_job          — boto3 start_job_run; one Spark application
                               produces all 3 outputs from a single source read.
  3. wait_for_emr            — reschedule-mode sensor; polls every 60s,
                               4h timeout, raises with EMR stateDetails on failure.
  4. create_iceberg_tables   — CREATE ICEBERG TABLE IF NOT EXISTS for each
                               of the 3 tables. Idempotent.
  5. refresh_iceberg_tables  — ALTER ICEBERG TABLE ... REFRESH for each
                               so Snowflake picks up the new Glue snapshots
                               immediately (no 30-60s catalog-poll wait).

End state:
  • 3 Iceberg tables in glue.taxi_iceberg.{daily_agg,
    supply_gaps, tip_behaviour} have the year's rows.
  • Same 3 tables in ANALYTICS.HISTORICAL queryable by ANALYST.

Trigger: DAGs → spark_historical → ▶ Trigger DAG w/ config → set `year`.
Pre-ingest: `make ingest MONTHS=YYYY` (or let ensure_year_in_s3 do it).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import DAG, Variable
from airflow.sdk.definitions.param import Param
from airflow.sensors.base import PokeReturnValue

log = logging.getLogger(__name__)

_ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")


def _send_failure_email(context: dict) -> None:
    """Send a failure-alert email via direct smtplib.

    Sidesteps Airflow 3.0.x's broken default failure-email template
    (references `ti.mark_success_url` which doesn't exist on
    RuntimeTaskInstance, crashing the send). See dbt_pipeline.py for the
    full reasoning. Idempotent + safe: silently no-ops if ALERT_EMAIL is
    unset and catches all SMTP exceptions.
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
    "on_failure_callback": [_send_failure_email] if _ALERT_EMAIL else [],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Snowflake connection — DBT role owns HISTORICAL schema and has USAGE on
# the EXTERNAL VOLUME + CATALOG INTEGRATION (see infra/rbac.tf).
SF_DBT_CONN_ID = "snowflake_dbt"

# The 3 Iceberg tables Spark writes. Each tuple is
# (Snowflake table name, Glue catalog table name). Keep these in sync with
# the TABLE_* constants in spark/process_historical.py — changing either
# requires a coordinated Glue + Snowflake update.
SF_HISTORICAL_DB = "ANALYTICS"
SF_HISTORICAL_SCHEMA = "HISTORICAL"
HISTORICAL_TABLES: list[tuple[str, str]] = [
    ("DAILY_AGG", "daily_agg"),
    ("SUPPLY_GAPS", "supply_gaps"),
    ("TIP_BEHAVIOUR", "tip_behaviour"),
]


def _sf_fqtn(snowflake_table: str) -> str:
    return f"{SF_HISTORICAL_DB}.{SF_HISTORICAL_SCHEMA}.{snowflake_table}"


with DAG(
    dag_id="spark_historical",
    description=(
        "Manual-trigger DAG. Submits process_historical.py to EMR Serverless, "
        "which writes an Iceberg table registered in AWS Glue. Snowflake reads "
        "zero-copy via CATALOG INTEGRATION."
    ),
    start_date=datetime(2009, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["spark", "emr-serverless", "iceberg", "historical"],
    params={
        "year": Param(
            default=2023,
            type="integer",
            minimum=2009,
            maximum=datetime.now().year,
            title="Year to process",
            description=(
                "Which year of TLC parquet to pre-aggregate. One run = one "
                "year. For multi-year backfill, trigger N times — "
                "max_active_runs=1 serializes them. Re-runs of the same year "
                "are idempotent (the Spark job DELETEs the year's rows then "
                "appends fresh)."
            ),
        ),
    },
) as dag:

    @task(
        task_id="ensure_year_in_s3",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    def ensure_year_in_s3(**context) -> int:
        """Mirror the year's TLC parquet from CloudFront → S3 raw/.

        Spark can't read CloudFront directly; we stage to our own bucket
        first. HEAD-skip idempotent — re-runs are ~2s no-ops, not 3 GB
        re-downloads. Returns count uploaded (0 if all already present).
        """
        from ingestion.ingest_tlc import ingest_missing, months_for_year

        params = context["params"] or {}
        year = int(params["year"])
        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_raw_prefix", default_var="raw/")

        months = months_for_year(year)
        log.info("ingesting %d months for year=%d → s3://%s/%s", len(months), year, bucket, prefix)
        uploaded = ingest_missing(months, bucket=bucket, prefix=prefix)
        log.info(
            "ingest done: uploaded=%d (the rest were already present in S3)",
            len(uploaded),
        )
        return len(uploaded)

    @task(task_id="submit_emr_job")
    def submit_emr_job(**context) -> str:
        """Submit process_historical.py to EMR Serverless. Returns job_run_id.

        Spark conf wires up the Iceberg + Glue catalog so the script can
        write all 3 tables under `glue.taxi_iceberg.*`.
        """
        import boto3

        params = context["params"] or {}
        year = int(params["year"])
        bucket = Variable.get("s3_bucket")
        application_id = Variable.get("emr_application_id")
        exec_role_arn = Variable.get("emr_exec_role_arn")
        glue_database = Variable.get("glue_database")

        entry_point = f"s3://{bucket}/spark-scripts/process_historical.py"
        # Read from our own raw/ — TLC's primary distribution is CloudFront
        # (used by ingestion/ingest_tlc.py); the legacy s3://nyc-tlc/ mirror
        # blocks cross-account reads. Pre-ingest via `make ingest MONTHS=YYYY`
        # or via dbt_pipeline. Override via the `tlc_source` Airflow Variable.
        input_path = Variable.get("tlc_source", default_var=f"s3://{bucket}/raw/")
        warehouse = f"s3://{bucket}/historical-daily/"

        log.info(
            "submitting EMR job: app=%s year=%d catalog=glue.%s (3 tables)",
            application_id,
            year,
            glue_database,
        )

        client = boto3.client("emr-serverless")
        response = client.start_job_run(
            applicationId=application_id,
            executionRoleArn=exec_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": entry_point,
                    "entryPointArguments": [
                        "--input",
                        input_path,
                        "--catalog",
                        "glue",
                        "--database",
                        glue_database,
                        "--year",
                        str(year),
                    ],
                    "sparkSubmitParameters": (
                        # Iceberg + Glue catalog config (EMR 7.x bundles JARs)
                        "--conf spark.sql.extensions="
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                        "--conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog "
                        "--conf spark.sql.catalog.glue.catalog-impl="
                        "org.apache.iceberg.aws.glue.GlueCatalog "
                        "--conf spark.sql.catalog.glue.io-impl="
                        "org.apache.iceberg.aws.s3.S3FileIO "
                        f"--conf spark.sql.catalog.glue.warehouse={warehouse} "
                        # Resource sizing — fits inside the application's
                        # 16 vCPU / 64 GB max capacity (see infra/emr.tf):
                        # 4-core driver + 3 × 4-core executors = 16 vCPU,
                        # 16 GB driver + 3 × 8 GB executors = 40 GB.
                        # Plenty of headroom for one year of TLC parquet.
                        "--conf spark.executor.cores=4 "
                        "--conf spark.executor.memory=8g "
                        "--conf spark.executor.instances=3 "
                        "--conf spark.driver.cores=4 "
                        "--conf spark.driver.memory=16g "
                        # AQE
                        "--conf spark.sql.adaptive.enabled=true "
                        "--conf spark.sql.adaptive.coalescePartitions.enabled=true "
                        "--conf spark.sql.adaptive.skewJoin.enabled=true "
                        # TLC schema-drift handling: VendorID and a few
                        # other columns flip between INT32 and BIGINT across
                        # months/years in TLC's parquet output. The default
                        # vectorized reader is strict about exact type
                        # matches and rejects the file. The non-vectorized
                        # path silently widens INT32 → BIGINT, which is the
                        # behaviour we want for the union-of-files read.
                        # ~10-20% slower reads; acceptable trade-off.
                        "--conf spark.sql.parquet.enableVectorizedReader=false"
                    ),
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/spark-logs/"}
                }
            },
            name=f"historical-{year}",
        )
        job_run_id = response["jobRunId"]
        log.info("submitted EMR job_run_id=%s for year=%d", job_run_id, year)
        return job_run_id

    @task.sensor(
        task_id="wait_for_emr",
        poke_interval=60,
        timeout=60 * 60 * 4,
        mode="reschedule",
        retries=0,
    )
    def wait_for_emr(job_run_id: str, **context) -> PokeReturnValue:
        """Poll EMR job state until terminal. mode=reschedule frees the slot
        between pokes — a 30-min Spark job doesn't tie up Airflow."""
        import boto3

        application_id = Variable.get("emr_application_id")
        client = boto3.client("emr-serverless")
        result = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)
        state = result["jobRun"]["state"]
        log.info("EMR job %s state=%s", job_run_id, state)

        if state == "SUCCESS":
            return PokeReturnValue(is_done=True, xcom_value=state)
        if state in ("FAILED", "CANCELLED"):
            details = result["jobRun"].get("stateDetails", "no detail")
            raise AirflowException(f"EMR job {job_run_id} ended in state {state}: {details}")
        return PokeReturnValue(is_done=False)

    @task(task_id="create_iceberg_tables")
    def create_iceberg_tables() -> None:
        """CREATE ICEBERG TABLE IF NOT EXISTS for each of the 3 historical
        tables. Idempotent. Each table is bound to its Glue catalog entry
        via CATALOG_TABLE_NAME — Spark already registered them in Glue."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        external_volume = Variable.get("snowflake_external_volume")
        catalog_integration = Variable.get("snowflake_catalog_integration")
        glue_database = Variable.get("glue_database")

        hook = SnowflakeHook(snowflake_conn_id=SF_DBT_CONN_ID)
        for sf_table, glue_table in HISTORICAL_TABLES:
            fqtn = _sf_fqtn(sf_table)
            sql = f"""
                CREATE ICEBERG TABLE IF NOT EXISTS {fqtn}
                EXTERNAL_VOLUME       = '{external_volume}'
                CATALOG               = '{catalog_integration}'
                CATALOG_TABLE_NAME    = '{glue_table}'
                CATALOG_NAMESPACE     = '{glue_database}'
                AUTO_REFRESH          = TRUE
            """
            log.info("ensuring Iceberg table %s exists (glue=%s)", fqtn, glue_table)
            hook.run(sql)

    @task(task_id="refresh_iceberg_tables")
    def refresh_iceberg_tables() -> None:
        """ALTER ICEBERG TABLE ... REFRESH for each — pulls the latest Glue
        snapshot immediately, no 30-60s catalog-poll wait. Idempotent."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SF_DBT_CONN_ID)
        for sf_table, _ in HISTORICAL_TABLES:
            fqtn = _sf_fqtn(sf_table)
            log.info("refreshing Iceberg snapshot for %s", fqtn)
            hook.run(f"ALTER ICEBERG TABLE {fqtn} REFRESH")

    # ---- wiring -----------------------------------------------------------
    # ensure_year_in_s3 → submit_emr_job → wait_for_emr
    #     → create_iceberg_tables → refresh_iceberg_tables
    # Sequential by data-dependency. The two trailing tasks each loop over
    # all 3 historical tables internally, keeping the topology unchanged.

    ingested = ensure_year_in_s3()
    job_id = submit_emr_job()
    waited = wait_for_emr(job_id)
    created = create_iceberg_tables()
    refreshed = refresh_iceberg_tables()

    ingested >> job_id >> waited >> created >> refreshed
