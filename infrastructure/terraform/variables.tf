variable "PROJECT_NAME" {
  type    = string
  default = "sales-data-pipeline"
}
variable "ENVIRONMENT" {
  type    = string
  default = "dev"
}
variable "AWS_REGION" {
  type    = string
  default = "us-east-1"
}

variable "ALLOWED_CIDR" { type = string }          # e.g. <YOUR_IP/32>

variable "RAW_BUCKET" {
  type        = string
  description = "S3 bucket for raw data storage"
}
variable "PROCESSED_BUCKET" {
  type        = string
  description = "S3 bucket for processed data storage"
}

variable "TRUSTED_PRINCIPAL_ARN" {
  type        = string
  default     = ""
  description = "ARN of principal that can assume IAM roles (optional)"
}

# Kafka/MSK Configuration
variable "ENABLE_MSK" {
  type        = bool
  default     = false
  description = "Enable AWS MSK cluster (set to false for Docker-only development)"
}

variable "KAFKA_VERSION" {
  type        = string
  default     = "2.8.1"
  description = "Kafka version for MSK cluster"
}

variable "BROKER_INSTANCE_TYPE" {
  type        = string
  default     = "kafka.t3.small"
  description = "Instance type for MSK brokers"
}

variable "NUMBER_OF_BROKERS" {
  type        = number
  default     = 3
  description = "Number of broker nodes in MSK cluster"
}

# Snowflake Configuration
variable "ENABLE_SNOWFLAKE_OBJECTS" {
  type        = bool
  default     = true
  description = "Enable Snowflake object creation via Terraform"
}

variable "SNOWFLAKE_ACCOUNT_NAME" {
  type        = string
  default     = "YOUR_ACCOUNT_NAME"
  description = "Snowflake account name (without domain suffix)"
}

variable "SNOWFLAKE_ORGANIZATION_NAME" {
  type        = string
  default     = "YOUR_ORGANIZATION_NAME"
  description = "Snowflake organization name"
}

variable "SNOWFLAKE_USER" {
  type        = string
  default     = "TERRAFORM_USER"
  description = "Snowflake user for Terraform operations"
}

variable "SNOWFLAKE_PASSWORD" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Snowflake password for authentication (legacy - use keypair instead)"
}

variable "SNOWFLAKE_PRIVATE_KEY_PATH" {
  type        = string
  default     = ""
  description = "Path to Snowflake RSA private key for keypair authentication"
}

variable "SNOWFLAKE_KEY_PASSPHRASE" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Passphrase for encrypted Snowflake private key"
}

variable "SNOWFLAKE_ROLE" {
  type        = string
  default     = "ACCOUNTADMIN"
  description = "Snowflake role for Terraform operations"
}

variable "SNOWFLAKE_IAM_USER_ARN" {
  type        = string
  default     = ""
  description = "Snowflake IAM user ARN for S3 access (stable per Snowflake account)"
}

variable "SNOWFLAKE_EXTERNAL_ID" {
  type        = string
  default     = ""
  description = "External ID for Snowflake IAM role trust policy"
}
