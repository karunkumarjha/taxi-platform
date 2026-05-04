# IAM role assumed by Snowflake's storage integration.
# Trust policy references the auto-generated external_id + IAM user ARN that Snowflake
# publishes after the storage_integration resource is created. Because the integration
# was created with a predicted ARN (see snowflake.tf), there is no dependency cycle.

data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.tlc_s3.storage_aws_iam_user_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration.tlc_s3.storage_aws_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_s3" {
  name               = local.snowflake_role_name
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
  tags               = local.tags
}

data "aws_iam_policy_document" "snowflake_s3_read" {
  statement {
    sid       = "ListBucketRaw"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "raw/"]
    }
  }

  statement {
    sid     = "ReadRawObjects"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/raw/*",
    ]
  }
}

resource "aws_iam_role_policy" "snowflake_s3_read" {
  name   = "${local.snowflake_role_name}-read"
  role   = aws_iam_role.snowflake_s3.id
  policy = data.aws_iam_policy_document.snowflake_s3_read.json
}

# ============================================================================
# Iceberg + Glue access roles for Snowflake
# ============================================================================
# Snowflake reads the Iceberg historical-daily table via a CATALOG INTEGRATION
# (Glue metadata) + EXTERNAL VOLUME (S3 data files). Each one creates a
# Snowflake-side IAM principal that needs to assume an AWS role.
#
# We use ONE combined role for both — Snowflake's CATALOG INTEGRATION and
# EXTERNAL VOLUME each expose their own IAM user ARN, and the trust policy
# below permits both. Single role, single policy, less drift than splitting.

locals {
  snowflake_iceberg_role_name = "${var.project_name}-snowflake-iceberg"
  predicted_iceberg_role      = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.snowflake_iceberg_role_name}"
}

data "aws_iam_policy_document" "snowflake_iceberg_trust" {
  # Snowflake creates ONE account-scoped IAM user shared across all
  # integrations (storage integration, external volume, catalog integration).
  # That ARN is exposed by the existing storage integration resource — same
  # principal used by both the EXTERNAL VOLUME and (post-create) the
  # CATALOG INTEGRATION. The external IDs differ per integration, but
  # without TF attributes for the catalog integration (created via
  # snowflake_execute), we trust the IAM user identity without strict
  # external-ID matching. Acceptable security posture for a personal
  # project — the IAM user is itself account-scoped to this Snowflake
  # account, so the trust effectively means "any integration in this
  # Snowflake account can assume the role."
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.tlc_s3.storage_aws_iam_user_arn]
    }
  }
}

resource "aws_iam_role" "snowflake_iceberg" {
  name               = local.snowflake_iceberg_role_name
  assume_role_policy = data.aws_iam_policy_document.snowflake_iceberg_trust.json
  tags               = local.tags
}

# Permissions: read S3 historical-daily/ (Iceberg data + manifest files) +
# read Glue catalog metadata (Iceberg snapshot pointer).
data "aws_iam_policy_document" "snowflake_iceberg" {
  statement {
    sid       = "ListBucketHistorical"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["historical-daily/*", "historical-daily/"]
    }
  }

  statement {
    sid     = "ReadIcebergFiles"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/historical-daily/*",
    ]
  }

  statement {
    sid    = "ReadGlueCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.taxi_iceberg.arn,
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.taxi_iceberg.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "snowflake_iceberg" {
  name   = "${local.snowflake_iceberg_role_name}-read"
  role   = aws_iam_role.snowflake_iceberg.id
  policy = data.aws_iam_policy_document.snowflake_iceberg.json
}
