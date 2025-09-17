# infra/modules/snowflake/variables.tf

variable "PROJECT_NAME" {
  description = "Name of the project"
  type        = string
}

variable "ENVIRONMENT" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}
