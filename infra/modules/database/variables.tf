variable "PROJECT_NAME" { type = string }
variable "DB_NAME"      { type = string }
variable "DB_USERNAME"  { type = string }
variable "DB_PASSWORD"  { type = string }
variable "DB_INSTANCE_CLASS"    { type = string }
variable "DB_ALLOCATED_STORAGE" { type = number }
variable "DB_ENGINE_VERSION"    { type = string }
variable "DB_BACKUP_RETENTION_DAYS" { type = number }
variable "PUBLICLY_ACCESSIBLE"  { type = bool }

variable "VPC_ID"     { type = string }
variable "SUBNET_IDS" { type = list(string) }
variable "RDS_SG_ID"  { type = string }
