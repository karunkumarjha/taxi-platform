# EMR Serverless application + IAM execution role for the historical
# scale-time Spark workload (spark/process_historical.py).
#
# Why EMR Serverless (not classic EMR or Glue):
#   • pre-init capacity = 0  →  zero idle cost; perfect for the bursty
#     once-per-year-per-historical-year cadence
#   • no cluster lifecycle  →  no SSH keys, security groups, bootstrap
#   • per-job IAM           →  this exec role scopes S3 access exactly
#     to the prefixes Spark needs to read/write
#
# The Airflow `spark_historical` DAG calls
#   boto3.client('emr-serverless').start_job_run(...)
# with the application_id and exec_role_arn from the outputs below.

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-historical-spark"
  type          = "spark"
  release_label = "emr-7.2.0" # Spark 3.5.x; AQE on by default

  # No initial_capacity block → pre-init = 0 → workers are provisioned only
  # when a job actually starts. Cold-start ~30–60s on the first job after
  # idle, but $0 idle cost — perfect for the ad-hoc once-per-historical-year
  # cadence.
  #
  # `maximum_capacity` caps the total pool the application can scale into.
  # 16 vCPU / 64 GB sits inside the default per-account vCPU quota for
  # most AWS accounts (including new / trial / free-tier). Bump these
  # explicitly via the AWS Service Quotas console + `terraform apply` if
  # you want larger jobs (default quota is "Maximum concurrent vCPUs per
  # account" — request raise to e.g. 64 vCPU for production scale).
  #
  # The Spark conf in spark_historical DAG asks for 4-core / 16 GB
  # executors, so 16 vCPU / 64 GB allows up to 3 concurrent executors plus
  # a 4-core driver — sufficient for a single year of TLC parquet
  # (~1 GB compressed → ~10 GB shuffle).
  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }

  tags = local.tags
}

# IAM trust policy — only EMR Serverless can assume this role.
data "aws_iam_policy_document" "emr_exec_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_exec" {
  name               = "${var.project_name}-emr-exec"
  assume_role_policy = data.aws_iam_policy_document.emr_exec_trust.json
  tags               = local.tags
}

# Permissions: Spark reads raw + spark-scripts (the script itself), writes
# historical-daily output, writes job logs.
data "aws_iam_policy_document" "emr_exec" {
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid     = "ReadInputs"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/raw/*",
      "${aws_s3_bucket.data.arn}/spark-scripts/*",
      # historical-daily/* must be readable too: Iceberg's commit logic
      # reads existing manifests to compute the next snapshot ID, and
      # `DELETE FROM <iceberg_table> WHERE …` scans the partition's
      # manifests before issuing the deletion. Without GetObject here,
      # second-and-subsequent runs (and any year re-run) hit AccessDenied.
      "${aws_s3_bucket.data.arn}/historical-daily/*",
    ]
  }

  statement {
    sid    = "WriteOutputsAndLogs"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      # Iceberg's S3FileIO uses multipart uploads for files >5 MB; it
      # needs to list parts to commit. Mostly transparent but required.
      "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/historical-daily/*",
      "${aws_s3_bucket.data.arn}/spark-logs/*",
    ]
  }

  # NYC TLC's public S3 bucket — for production runs that read directly
  # from s3://nyc-tlc/ instead of our copy in raw/. Optional but cheap to
  # grant (read-only on a public bucket).
  statement {
    sid     = "ReadPublicTLC"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      "arn:aws:s3:::nyc-tlc",
      "arn:aws:s3:::nyc-tlc/*",
    ]
  }

  # Glue catalog R/W — Iceberg writes register manifest changes here on
  # commit. CreateTable / UpdateTable handle the table-level snapshot
  # pointer; CreatePartition / UpdatePartition handle partition-level
  # metadata for partitioned tables. GetCatalogImportStatus is required
  # for client init.
  statement {
    sid    = "GlueCatalogReadWrite"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:GetCatalogImportStatus",
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.taxi_iceberg.arn,
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.taxi_iceberg.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "emr_exec" {
  name   = "${var.project_name}-emr-exec"
  role   = aws_iam_role.emr_exec.id
  policy = data.aws_iam_policy_document.emr_exec.json
}
