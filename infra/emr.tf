# Phase 2 — EMR Serverless application and its job-run exec role.
#
# EMR Serverless has zero idle cost (pre-init capacity = 0), so it's safe to
# leave provisioned in the shared stack — the job-run IAM role + the application
# itself are free until we actually submit a job. Submission happens via
# spark/submit_emr.py (called from Airflow or the CLI).

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark"
  release_label = "emr-7.3.0" # includes Spark 3.5
  type          = "spark"

  architecture = "X86_64"

  # Idle cost controls: no pre-initialised capacity means cold start on first
  # job, but the project doesn't need <1-min latency — we pay $0 while idle.
  maximum_capacity {
    cpu    = "32 vCPU"
    memory = "128 GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }

  tags = local.tags
}

# --- IAM role for EMR Serverless job runs ------------------------------------

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

data "aws_iam_policy_document" "emr_exec_policy" {
  # Read the raw + scripts prefixes, write analytics + logs.
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]
  }
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/raw/*",
      "${aws_s3_bucket.data.arn}/spark-scripts/*",
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/analytics/*",
      "${aws_s3_bucket.data.arn}/spark-logs/*",
    ]
  }
  # CloudWatch log streaming for the driver — useful for debugging from the EMR console.
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_exec" {
  name   = "${var.project_name}-emr-exec-policy"
  role   = aws_iam_role.emr_exec.id
  policy = data.aws_iam_policy_document.emr_exec_policy.json
}
