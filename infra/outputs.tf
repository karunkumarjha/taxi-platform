output "s3_bucket" {
  description = "Data bucket name (single bucket, prefixes inside)"
  value       = aws_s3_bucket.data.bucket
}

output "s3_raw_uri" {
  description = "s3:// URI of the raw landing prefix"
  value       = "s3://${aws_s3_bucket.data.bucket}/raw/"
}

output "aws_region" {
  description = "AWS region"
  value       = data.aws_region.current.name
}

output "snowflake_database" {
  value = snowflake_database.analytics.name
}

output "snowflake_raw_schema" {
  value = snowflake_schema.raw.name
}

output "snowflake_warehouse" {
  value = snowflake_warehouse.wh_xs.name
}

output "snowflake_stage" {
  description = "Fully-qualified stage name for COPY INTO"
  value       = "${snowflake_database.analytics.name}.${snowflake_schema.raw.name}.${snowflake_stage.s3_tlc_stage.name}"
}

output "snowflake_storage_integration" {
  value = snowflake_storage_integration.tlc_s3.name
}

output "snowflake_s3_role_arn" {
  description = "IAM role Snowflake assumes to read s3://<bucket>/raw/"
  value       = aws_iam_role.snowflake_s3.arn
}

output "snowflake_external_id" {
  description = "External ID Snowflake uses in the sts:AssumeRole call (useful for manual trust-policy debugging)"
  value       = snowflake_storage_integration.tlc_s3.storage_aws_external_id
  sensitive   = false
}

# --- Phase 2 outputs ---------------------------------------------------------

output "s3_analytics_uri" {
  description = "s3:// URI where Spark writes daily aggregations"
  value       = "s3://${aws_s3_bucket.data.bucket}/analytics/"
}

output "s3_spark_scripts_uri" {
  value = "s3://${aws_s3_bucket.data.bucket}/spark-scripts/"
}

output "s3_spark_logs_uri" {
  value = "s3://${aws_s3_bucket.data.bucket}/spark-logs/"
}

output "emr_application_id" {
  description = "EMR Serverless application ID (pass to start_job_run)"
  value       = aws_emrserverless_application.spark.id
}

output "emr_exec_role_arn" {
  description = "IAM role EMR Serverless job runs assume"
  value       = aws_iam_role.emr_exec.arn
}

# --- RBAC outputs -----------------------------------------------------------
# Users + their roles. Passwords are sensitive — read with:
#   terraform output -raw snowflake_dbt_password

output "snowflake_loader_user" {
  value = snowflake_user.loader.name
}
output "snowflake_loader_role" {
  value = snowflake_account_role.loader.name
}
output "snowflake_loader_password" {
  value     = random_password.loader.result
  sensitive = true
}

output "snowflake_dbt_user" {
  value = snowflake_user.dbt.name
}
output "snowflake_dbt_role" {
  value = snowflake_account_role.dbt.name
}
output "snowflake_dbt_password" {
  value     = random_password.dbt.result
  sensitive = true
}

output "snowflake_dashboard_user" {
  value = snowflake_user.dashboard.name
}
output "snowflake_dashboard_role" {
  value = snowflake_account_role.dashboard.name
}
output "snowflake_dashboard_password" {
  value     = random_password.dashboard.result
  sensitive = true
}

output "snowflake_analyst_user" {
  value = snowflake_user.analyst.name
}
output "snowflake_analyst_role" {
  value = snowflake_account_role.analyst.name
}
output "snowflake_analyst_password" {
  value     = random_password.analyst.result
  sensitive = true
}

# --- Streamlit-in-Snowflake -------------------------------------------------

output "streamlit_app" {
  description = "Fully-qualified Streamlit app name (open via Snowsight → Streamlit)"
  value       = "${snowflake_database.analytics.name}.${snowflake_schema.marts.name}.${snowflake_streamlit.analytics_app.name}"
}

output "streamlit_app_stage" {
  description = "Stage that hosts the Streamlit source files. PUT new code here."
  value       = "${snowflake_database.analytics.name}.${snowflake_schema.marts.name}.${snowflake_stage.streamlit_app.name}"
}
