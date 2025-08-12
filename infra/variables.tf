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
