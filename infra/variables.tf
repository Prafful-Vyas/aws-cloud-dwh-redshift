variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used to prefix/tag all resources."
  type        = string
  default     = "olist-dwh"
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for raw/processed Olist data and manifests. Must be changed from the example before applying."
  type        = string
}

variable "redshift_admin_username" {
  description = "Admin username for the Redshift Serverless namespace."
  type        = string
  default     = "admin"
}

variable "redshift_base_capacity" {
  description = "Base RPU capacity for the Redshift Serverless workgroup."
  type        = number
  default     = 8
}

variable "alert_email" {
  description = "Optional email to subscribe to the pipeline failure/success SNS topic."
  type        = string
  default     = ""
}
