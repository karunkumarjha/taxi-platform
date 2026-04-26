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
  comment = "Read-only access to s3://<bucket>/raw/ for RAW.YELLOW_TRIPDATA"
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
