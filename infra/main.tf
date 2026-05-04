terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Snowflake provider reads credentials from env vars:
#   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE
# See infra/README.md for the service-user bootstrap.
#
# `warehouse` is the *bootstrap* warehouse the provider uses to issue DDL while
# Terraform stands up the project's own WH_XS warehouse. Defaults to
# COMPUTE_WH which Snowflake creates in every fresh trial. If your account
# doesn't have COMPUTE_WH (older trials), create one with the SQL in
# infra/README.md or override `snowflake_bootstrap_warehouse`.
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  role              = var.snowflake_role
  warehouse         = var.snowflake_bootstrap_warehouse

  # Several resources are still flagged "preview" in the v1.x track even
  # though they're stable enough for production use. We opt into the
  # specific ones this project depends on; the provider is otherwise GA.
  preview_features_enabled = [
    "snowflake_file_format_resource",
    "snowflake_storage_integration_resource",
    "snowflake_external_volume_resource",
    "snowflake_stage_resource",
  ]
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "random_string" "bucket_suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  bucket_name              = "${var.project_name}-data-${coalesce(var.bucket_suffix, random_string.bucket_suffix.result)}"
  snowflake_role_name      = "${var.project_name}-snowflake-s3"
  predicted_snowflake_role = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.snowflake_role_name}"
  tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
  }
}
