"""
spark_historical — manual-trigger DAG. Submits process_historical.py to
EMR Serverless, which writes an Iceberg table registered in AWS Glue.
Snowflake reads zero-copy via CATALOG INTEGRATION.

The DAG itself does NOT run Spark. Airflow is the control plane only —
compute happens on EMR Serverless under the IAM exec role provisioned
by `infra/emr.tf`. The DAG is a 5-task wrapper:

  1. ensure_year_in_s3       — pre-ingest the year's 12 parquet files
                               from TLC CloudFront → S3 raw/ via the
                               existing ingestion module. Idempotent
                               (HEAD-skip): re-runs are quick no-ops
  2. submit_emr_job          — boto3 start_job_run (Spark runs on EMR)
  3. wait_for_emr            — sensor in `mode = reschedule` so the
                               worker slot is FREED between pokes;
                               polls job state every 60s until terminal
  4. create_iceberg_table    — CREATE ICEBERG TABLE IF NOT EXISTS in
                               Snowflake. Idempotent — first run
                               creates, subsequent runs are no-ops
  5. refresh_iceberg         — ALTER ICEBERG TABLE ... REFRESH so
                               Snowflake picks up the new Glue snapshot
                               immediately (without waiting for the
                               30s catalog poll)

Trigger via UI:
  1. DAGs → spark_historical → ▶ Trigger DAG w/ config
  2. Set Param `year` (default 2023; valid 2009–current year)
  3. Trigger.

End state on success:
  • Iceberg table glue.taxi_iceberg.historical_daily has the year's
    daily-aggregation rows (replacing any previous data for that year)
  • Snowflake table ANALYTICS.HISTORICAL.HISTORICAL_DAILY_AGG queryable
    by the ANALYST role
  • Daily-aggregation parquet files at
    s3://<bucket>/historical-daily/<table>/data/year=YYYY/month=MM/

Input source:
  By default, the DAG reads from `s3://<bucket>/raw/` — the same
  prefix `dbt_pipeline` and `make ingest` write to. Pre-ingest the year
  before triggering:

      make ingest MONTHS=2023            # downloads all 12 months of 2023

  TLC's primary source is the CloudFront URL (used by ingest_tlc.py);
  the legacy public S3 mirror at s3://nyc-tlc/ is no longer reliably
  accessible from arbitrary AWS accounts. Override via the `tlc_source`
  Airflow Variable if you have a different layout.

Why no schedule:
  Historical pre-aggregation is a backfill-style operation, not a
  recurring one. Each year is run once when needed. Live monthly data
  flows through dbt_pipeline; only 14+ years of historical bulk go
  through this DAG.

Resilience:
  • Submit task — retries=2, retry_delay=5min for transient AWS API hiccups
  • Sensor — mode=reschedule frees the worker between pokes; 4h timeout
  • Sensor failure (FAILED / CANCELLED) raises immediately with the
    EMR stateDetails string in the error message
  • Refresh task is idempotent — safe to retry
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

# Iceberg table location in Snowflake — fixed names. Could be wired through
# Airflow Variables but they're stable infra-side and changing them would
# require coordinated Glue + Snowflake updates.
SF_HISTORICAL_DB = "ANALYTICS"
SF_HISTORICAL_SCHEMA = "HISTORICAL"
SF_HISTORICAL_TABLE = "HISTORICAL_DAILY_AGG"
SF_HISTORICAL_FQTN = f"{SF_HISTORICAL_DB}.{SF_HISTORICAL_SCHEMA}.{SF_HISTORICAL_TABLE}"


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
        """Pre-ingest all months of the year from TLC CloudFront → S3 raw/.

        Why this is the first task: TLC's primary distribution is now
        CloudFront (their public S3 mirror was retired); Spark can't read
        CloudFront directly. We materialise the year's parquet into our
        own bucket, then EMR reads from there.

        Idempotent — `ingest_missing` HEAD-checks each S3 key first and
        skips the upload if already present. Re-running the DAG for a
        year that's already been ingested is a quick no-op (12 HEADs,
        ~2s) rather than re-downloading ~3 GB.

        Returns the count of files actually uploaded (0 if all already
        present). Logged for visibility but not used downstream.
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
        CREATE TABLE / DELETE / APPEND against `glue.taxi_iceberg.historical_daily`.
        """
        import boto3

        params = context["params"] or {}
        year = int(params["year"])
        bucket = Variable.get("s3_bucket")
        application_id = Variable.get("emr_application_id")
        exec_role_arn = Variable.get("emr_exec_role_arn")
        glue_database = Variable.get("glue_database")

        entry_point = f"s3://{bucket}/spark-scripts/process_historical.py"
        # Read from our own raw/ prefix. TLC's official primary source is
        # now their CloudFront URL (the `ingestion/ingest_tlc.py` script
        # uses it); the legacy `s3://nyc-tlc/*` mirror is no longer
        # reliably accessible from arbitrary AWS accounts (returns 403
        # even with full IAM grants — the bucket policy blocks
        # cross-account reads).
        #
        # Workflow: pre-ingest the year via `make ingest MONTHS=YYYY`
        # before triggering this DAG, OR via the dbt_pipeline live path
        # over time. Each year's 12 monthly parquet files land in
        # s3://<bucket>/raw/ and Spark reads them from there.
        #
        # Override via the `tlc_source` Airflow Variable if you have a
        # different layout (e.g. an internal data-lake mirror).
        input_path = Variable.get("tlc_source", default_var=f"s3://{bucket}/raw/")
        warehouse = f"s3://{bucket}/historical-daily/"

        log.info(
            "submitting EMR job: app=%s year=%d catalog=glue.%s.historical_daily",
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
                        "--table",
                        "historical_daily",
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
            name=f"historical-daily-{year}",
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

    @task(task_id="create_iceberg_table")
    def create_iceberg_table() -> None:
        """Idempotent CREATE ICEBERG TABLE IF NOT EXISTS in Snowflake.

        Binds Snowflake's table to the Glue catalog entry that Spark just
        registered. First run creates; subsequent runs are no-ops thanks
        to IF NOT EXISTS. Safe to retry.

        The table is `external_volume`-bound and `catalog`-bound to the
        Snowflake objects we provisioned in Terraform. Snowflake reads
        the manifest pointer from Glue and the data files from the
        EXTERNAL VOLUME's S3 prefix — no copy.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        external_volume = Variable.get("snowflake_external_volume")
        catalog_integration = Variable.get("snowflake_catalog_integration")
        glue_database = Variable.get("glue_database")

        sql = f"""
            CREATE ICEBERG TABLE IF NOT EXISTS {SF_HISTORICAL_FQTN}
            EXTERNAL_VOLUME       = '{external_volume}'
            CATALOG               = '{catalog_integration}'
            CATALOG_TABLE_NAME    = 'historical_daily'
            CATALOG_NAMESPACE     = '{glue_database}'
            AUTO_REFRESH          = TRUE
        """
        log.info("ensuring Iceberg table %s exists", SF_HISTORICAL_FQTN)
        SnowflakeHook(snowflake_conn_id=SF_DBT_CONN_ID).run(sql)

    @task(task_id="refresh_iceberg")
    def refresh_iceberg() -> None:
        """ALTER ICEBERG TABLE ... REFRESH — pulls the latest snapshot pointer
        from Glue immediately. Eliminates the 30-60s polling lag from the
        catalog integration's auto-refresh; gives this DAG run a deterministic
        'data is visible' signal."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        sql = f"ALTER ICEBERG TABLE {SF_HISTORICAL_FQTN} REFRESH"
        log.info("refreshing Iceberg snapshot for %s", SF_HISTORICAL_FQTN)
        SnowflakeHook(snowflake_conn_id=SF_DBT_CONN_ID).run(sql)

    # ---- wiring -----------------------------------------------------------
    # ensure_year_in_s3 → submit_emr_job → wait_for_emr → create_iceberg_table → refresh_iceberg
    #
    # Sequential by data-dependency: EMR can't run until the year's
    # parquet is in S3; the Iceberg DDL can't run until EMR has
    # registered the Glue table; the REFRESH can't run until the table
    # exists in Snowflake.

    ingested = ensure_year_in_s3()
    job_id = submit_emr_job()
    waited = wait_for_emr(job_id)
    created = create_iceberg_table()
    refreshed = refresh_iceberg()

    ingested >> job_id >> waited >> created >> refreshed
