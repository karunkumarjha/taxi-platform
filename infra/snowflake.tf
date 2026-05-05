# Snowflake objects: database, schemas, warehouse, file format,
# storage integration → S3, and the external stage used by COPY INTO.

resource "snowflake_database" "analytics" {
  name    = "ANALYTICS"
  comment = "NYC TLC taxi pattern analysis — managed by terraform"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.analytics.name
  name     = "RAW"
  comment  = "Landing zone for TLC parquet via COPY INTO from external stage"
}

# Two-schema dbt layout (no separate STAGING schema). Everything dbt manages —
# staging views, atomic facts/dims, aggregate facts — lives in MARTS_BUILD
# during build, then atomically swaps into MARTS for the consumer-facing layer.
#
# SNAPSHOTS is a separate persistent schema for the SCD Type 2 snapshot table
# (snp_yellow_trips). It is NOT part of the blue-green swap — dbt snapshots
# write here directly and accumulate SCD history across all pipeline runs.
resource "snowflake_schema" "snapshots" {
  database = snowflake_database.analytics.name
  name     = "SNAPSHOTS"
  comment  = "SCD Type 2 snapshot layer — dbt snapshots write here directly; persists across blue-green swaps."
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.analytics.name
  name     = "MARTS"
  comment  = "PRODUCTION analytics layer — read by dashboards. Swapped from MARTS_BUILD on success."
}

resource "snowflake_schema" "marts_build" {
  database = snowflake_database.analytics.name
  name     = "MARTS_BUILD"
  comment  = "BUILD analytics layer — dbt writes here. Swapped into MARTS after tests pass."
}

# HISTORICAL schema holds the Iceberg `DAILY_AGG` table, populated
# by Spark on EMR Serverless and read by Snowflake via Glue CATALOG INTEGRATION.
# Lives in its own schema (NOT inside MARTS) so the blue-green
# `ALTER SCHEMA MARTS_BUILD SWAP WITH MARTS` can't accidentally move it —
# Spark writes are independent of the dbt build cycle and follow Iceberg's
# own atomic-snapshot semantics.
resource "snowflake_schema" "historical" {
  database = snowflake_database.analytics.name
  name     = "HISTORICAL"
  comment  = "Iceberg-backed historical pre-aggregation. Spark writes via Glue catalog; Snowflake reads zero-copy."
}

resource "snowflake_warehouse" "wh_xs" {
  name                = "WH_XS"
  warehouse_size      = var.snowflake_warehouse_size
  auto_suspend        = var.snowflake_warehouse_auto_suspend
  auto_resume         = true
  initially_suspended = true
  comment             = "XS warehouse for loader + dbt"
}

resource "snowflake_file_format" "parquet_ff" {
  name        = "PARQUET_FF"
  database    = snowflake_database.analytics.name
  schema      = snowflake_schema.raw.name
  format_type = "PARQUET"
  comment     = "TLC parquet format"
}

# Storage integration — predicts the IAM role ARN so the whole graph applies in one pass.
# Snowflake only validates the IAM trust policy at STAGE-USE time, not at integration creation.
resource "snowflake_storage_integration" "tlc_s3" {
  name    = "TLC_S3_INT"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "S3"
  storage_aws_role_arn = local.predicted_snowflake_role
  storage_allowed_locations = [
    "s3://${aws_s3_bucket.data.bucket}/raw/",
  ]
  comment = "Read access to raw/ for COPY INTO RAW.YELLOW_TRIPDATA"
}

resource "snowflake_stage" "s3_tlc_stage" {
  name                = "S3_TLC_STAGE"
  database            = snowflake_database.analytics.name
  schema              = snowflake_schema.raw.name
  url                 = "s3://${aws_s3_bucket.data.bucket}/raw/"
  storage_integration = snowflake_storage_integration.tlc_s3.name
  file_format         = "FORMAT_NAME = ${snowflake_database.analytics.name}.${snowflake_schema.raw.name}.${snowflake_file_format.parquet_ff.name}"
  comment             = "External stage over s3://<bucket>/raw/"
}

# ============================================================================
# Iceberg + Glue: EXTERNAL VOLUME + CATALOG INTEGRATION
# ============================================================================
# Predicted-ARN pattern (same as snowflake_storage_integration.tlc_s3):
# pass the IAM role's predicted ARN here so Snowflake can be created without
# the role existing yet, then the IAM role's trust policy references the
# IAM user that Snowflake exposes after creation. Single `terraform apply`
# brings the whole graph up.

resource "snowflake_external_volume" "historical" {
  name = "HISTORICAL_VOL"

  storage_location {
    storage_location_name = "historical-daily-s3"
    storage_provider      = "S3"
    storage_base_url      = "s3://${aws_s3_bucket.data.bucket}/historical-daily/"
    storage_aws_role_arn  = local.predicted_iceberg_role
  }

  # Snowflake reads only; EMR is the sole writer to historical-daily/.
  # Without this, CREATE ICEBERG TABLE issues a PutObject probe that 403s
  # against the read-only snowflake_iceberg IAM role (infra/iam.tf).
  allow_writes = "false"

  comment = "S3 location backing the Iceberg HISTORICAL.DAILY_AGG table."
}

# Catalog integration is created via raw SQL. The Snowflake Terraform
# provider at v1.2.3 doesn't have a `snowflake_catalog_integration` resource
# (yet), so we go through `snowflake_execute` — declarative-enough that
# `terraform destroy` cleans it up via the revert SQL.
#
# Refresh interval = 30s — polls Glue for snapshot pointer changes. The
# spark_historical DAG also calls ALTER ICEBERG TABLE ... REFRESH for
# deterministic visibility (Option 3: both auto + explicit).
resource "snowflake_execute" "glue_catalog_integration" {
  execute = <<-SQL
    CREATE CATALOG INTEGRATION IF NOT EXISTS GLUE_CATALOG
        CATALOG_SOURCE      = GLUE
        TABLE_FORMAT        = ICEBERG
        GLUE_AWS_ROLE_ARN   = '${local.predicted_iceberg_role}'
        GLUE_CATALOG_ID     = '${data.aws_caller_identity.current.account_id}'
        GLUE_REGION         = '${data.aws_region.current.name}'
        CATALOG_NAMESPACE   = '${aws_glue_catalog_database.taxi_iceberg.name}'
        REFRESH_INTERVAL_SECONDS = 30
        ENABLED             = TRUE
        COMMENT             = 'AWS Glue catalog integration for Iceberg DAILY_AGG'
  SQL

  revert = "DROP CATALOG INTEGRATION IF EXISTS GLUE_CATALOG"

  query = "SHOW CATALOG INTEGRATIONS LIKE 'GLUE_CATALOG'"
}

# The Iceberg table DAILY_AGG is created out-of-band by the
# spark_historical DAG (CREATE ICEBERG TABLE IF NOT EXISTS), so Terraform
# doesn't track it. Without an explicit cleanup, `terraform destroy` fails
# on the EXTERNAL VOLUME and CATALOG INTEGRATION because Snowflake refuses
# to drop either while a referencing Iceberg table still exists.
#
# Cleanup lives in the Makefile (`make infra-destroy`), which runs
# `dbt run-operation drop_historical_iceberg` AS THE DBT ROLE (the table's
# owner) before invoking terraform destroy. We can't do this via
# snowflake_execute because Terraform's snowflake provider runs as
# ACCOUNTADMIN, and Snowflake refuses to DROP an object owned by another
# role even from ACCOUNTADMIN unless ownership is explicitly transferred.

# Surface the catalog integration name as a local for outputs / DAG variables.
locals {
  glue_catalog_integration_name = "GLUE_CATALOG"
}
