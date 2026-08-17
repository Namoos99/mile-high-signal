# Denver 311 pipeline infrastructure: S3 raw/processed storage + IAM.
#
# ARCHITECTURE DECISIONS (see docs/DECISIONS.md AD-017):
#   - State is local (no backend block) deliberately, for a single-contributor
#     portfolio repo — a remote S3+DynamoDB backend is the right call the
#     moment a second person touches this, and is called out explicitly in
#     the README's "what I'd do differently at scale" section.
#   - The IAM policy is scoped to exactly this one bucket (Resource ARN, not a
#     wildcard). A pipeline identity that can write to every bucket in the
#     account is a much larger blast radius than this pipeline needs.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "raw_data" {
  bucket = var.bucket_name

  tags = {
    Project     = "denver311-pipeline"
    ManagedBy   = "terraform"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    # Versioning, not a substitute for the immutable-append-only convention in
    # landing.py (AD-003) — it's a safety net against accidental deletion, not
    # the primary mechanism for keeping history.
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket                  = aws_s3_bucket.raw_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "expire-old-manifests"
    status = "Enabled"

    filter {
      prefix = "raw/service_requests/_manifests/"
    }

    expiration {
      # Manifests are audit trail, not data — 1 year is generous for a
      # portfolio project and trivially adjustable for a real deployment.
      days = 365
    }
  }
}

# --- IAM: least-privilege role for the pipeline's own AWS access -----------

data "aws_iam_policy_document" "pipeline_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"] # adjust if run from ECS/Lambda/etc.
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.bucket_name}-pipeline-role"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume_role.json
}

data "aws_iam_policy_document" "pipeline_bucket_access" {
  statement {
    sid    = "ReadWriteThisBucketOnly"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.raw_data.arn,
      "${aws_s3_bucket.raw_data.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "pipeline_bucket_access" {
  name   = "${var.bucket_name}-bucket-access"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline_bucket_access.json
}
