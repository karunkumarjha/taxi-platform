variable "project_name" {
  description = "Generic project name — used as prefix for S3 bucket, IAM role, Snowflake objects"
  type        = string
  default     = "analytics"
}

variable "aws_region" {
  description = "AWS region for S3 + IAM"
  type        = string
  default     = "us-east-1"
}

variable "bucket_suffix" {
  description = "Optional override for the random suffix on the S3 bucket name. Leave blank to auto-generate."
  type        = string
  default     = ""
}

variable "snowflake_role" {
  description = "Snowflake role used by the Terraform provider (needs CREATE DATABASE + CREATE INTEGRATION)"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_bootstrap_warehouse" {
  description = <<-EOT
    Pre-existing warehouse the Terraform provider uses for its own session.
    Defaults to COMPUTE_WH (created automatically in every Snowflake trial).
    Project workloads use the WH_XS warehouse Terraform creates separately.
  EOT
  type        = string
  default     = "COMPUTE_WH"
}

# Snowflake account is supplied as two halves (organization + account name) to
# match the provider's new (non-deprecated) API. Both come from $SNOWFLAKE_ACCOUNT
# at the shell level — scripts/with_role.sh splits "ORG-ACCOUNT" before calling
# terraform, so users keep a single SNOWFLAKE_ACCOUNT entry in .env.
variable "snowflake_organization_name" {
  description = "Snowflake organization name (the part before the dash in your account locator)."
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name (the part after the dash in your account locator)."
  type        = string
}

variable "snowflake_warehouse_size" {
  description = "Size of the dbt / loader warehouse"
  type        = string
  default     = "XSMALL"
}

variable "snowflake_warehouse_auto_suspend" {
  description = "Warehouse auto-suspend in seconds"
  type        = number
  default     = 60
}
