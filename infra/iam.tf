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
  # List the raw/ + staged-marts/ prefixes.
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "raw/", "staged-marts/*", "staged-marts/"]
    }
  }

  # Read parquet objects under raw/ (TLC monthly files) and staged-marts/
  # (Spark-staged FCT_TRIPS / FCT_TRIPS_QUARANTINED partitions).
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/raw/*",
      "${aws_s3_bucket.data.arn}/staged-marts/*",
    ]
  }
}

resource "aws_iam_role_policy" "snowflake_s3_read" {
  name   = "${local.snowflake_role_name}-read"
  role   = aws_iam_role.snowflake_s3.id
  policy = data.aws_iam_policy_document.snowflake_s3_read.json
}
