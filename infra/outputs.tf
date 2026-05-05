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

output "snowflake_snapshots_schema" {
  description = "Schema where dbt snapshots (Silver SCD layer) live"
  value       = snowflake_schema.snapshots.name
}

output "snowflake_marts_schema" {
  description = "Production analytics schema (read by ANALYST + BI tools)"
  value       = snowflake_schema.marts.name
}

output "snowflake_marts_build_schema" {
  description = "Build-side schema for the blue-green dbt swap"
  value       = snowflake_schema.marts_build.name
}

output "snowflake_historical_schema" {
  description = "Schema holding the Iceberg DAILY_AGG table (read by ANALYST)"
  value       = snowflake_schema.historical.name
}

output "snowflake_external_volume" {
  description = "EXTERNAL VOLUME name bound to the Iceberg historical-daily/ S3 prefix"
  value       = snowflake_external_volume.historical.name
}

output "snowflake_catalog_integration" {
  description = "CATALOG INTEGRATION name pointing at the AWS Glue catalog (created via snowflake_execute)"
  value       = local.glue_catalog_integration_name
}

output "glue_database_name" {
  description = "AWS Glue Data Catalog database where Spark registers the Iceberg table"
  value       = aws_glue_catalog_database.taxi_iceberg.name
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

# --- EMR Serverless outputs --------------------------------------------------
# Used by the spark_historical Airflow DAG to start_job_run().

output "emr_application_id" {
  description = "EMR Serverless application ID for spark_historical DAG"
  value       = aws_emrserverless_application.spark.id
}

output "emr_exec_role_arn" {
  description = "IAM role EMR Serverless assumes when running process_historical.py"
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
