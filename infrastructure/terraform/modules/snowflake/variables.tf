# infra/modules/snowflake/variables.tf

variable "PROJECT_NAME" {
  description = "Name of the project"
  type        = string
}

variable "ENVIRONMENT" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "PROCESSED_BUCKET" {
  description = "S3 bucket name for processed/Silver layer data"
  type        = string
}

variable "snowflake_iam_role_arn" {
  description = "ARN of IAM role for Snowflake to assume for S3 access"
  type        = string
}

variable "ENABLE_SNOWFLAKE_OBJECTS" {
  description = "Enable creation of Snowflake objects (conditional deployment)"
  type        = bool
  default     = true
}

variable "SNOWFLAKE_EXTERNAL_ID" {
  description = "External ID for Snowflake IAM trust policy"
  type        = string
}

variable "SNOWFLAKE_USER" {
  description = "Snowflake user for Terraform operations"
  type        = string
}
