variable "PROJECT_NAME" { type = string }
variable "ENVIRONMENT"  { type = string }
variable "AWS_REGION"   { type = string }

variable "ALLOWED_CIDR" { type = string }          # e.g. <YOUR_IP/32>

variable "RAW_BUCKET"       { type = string }      # e.g. <PROJECT_NAME>-raw
variable "PROCESSED_BUCKET" { type = string }      # e.g. <PROJECT_NAME>-processed

variable "DB_NAME"                   { type = string }
variable "DB_USERNAME"               { type = string }
variable "DB_PASSWORD" {
  type      = string
  sensitive = true
}
variable "DB_INSTANCE_CLASS"         { type = string }
variable "DB_ALLOCATED_STORAGE"      { type = number }
variable "DB_ENGINE_VERSION"         { type = string }
variable "DB_BACKUP_RETENTION_DAYS"  { type = number }
variable "PUBLICLY_ACCESSIBLE"       { type = bool }

variable "TRUSTED_PRINCIPAL_ARN" { type = string } # who can assume the ETL role (optional now)

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
  default     = false
  description = "Enable Snowflake object creation via Terraform"
}

variable "SNOWFLAKE_ACCOUNT" {
  type        = string
  default     = "YOUR_ACCOUNT.us-east-1.aws"
  description = "Snowflake account identifier"
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
  description = "Snowflake password for Terraform operations"
}

variable "SNOWFLAKE_ROLE" {
  type        = string
  default     = "ACCOUNTADMIN"
  description = "Snowflake role for Terraform operations"
}

variable "SNOWFLAKE_REGION" {
  type        = string
  default     = "us-east-1"
  description = "Snowflake region"
}
