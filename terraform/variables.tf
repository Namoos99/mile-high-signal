variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-west-2"
}

variable "bucket_name" {
  description = "S3 bucket name for raw and processed 311 data. Must be globally unique."
  type        = string
  default     = "denver311-raw-prod" # rename before applying — S3 bucket names are global
}

variable "environment" {
  description = "Deployment environment tag."
  type        = string
  default     = "production"
}
