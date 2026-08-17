output "bucket_name" {
  value       = aws_s3_bucket.raw_data.bucket
  description = "Set this as S3_BUCKET in .env once applied."
}

output "bucket_arn" {
  value = aws_s3_bucket.raw_data.arn
}

output "pipeline_role_arn" {
  value       = aws_iam_role.pipeline.arn
  description = "Attach this role to whatever compute (EC2/ECS/Lambda) runs the ingestion job."
}
