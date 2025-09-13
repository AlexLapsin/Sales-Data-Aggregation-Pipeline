# legacy/infrastructure/terraform/main.tf
# ========================================
# ⚠️  DEPRECATION WARNING  ⚠️
# ========================================
# This infrastructure configuration is DEPRECATED
# and will be removed in Q3 2025.
# Please migrate to the modern cloud-native infrastructure.
# See legacy/README.md for migration guide.
# ========================================

# Legacy PostgreSQL RDS module configuration
module "database" {
  source                   = "./modules/database"
  PROJECT_NAME             = var.PROJECT_NAME
  DB_NAME                  = var.DB_NAME
  DB_USERNAME              = var.DB_USERNAME
  DB_PASSWORD              = var.DB_PASSWORD
  DB_INSTANCE_CLASS        = var.DB_INSTANCE_CLASS
  DB_ALLOCATED_STORAGE     = var.DB_ALLOCATED_STORAGE
  DB_ENGINE_VERSION        = var.DB_ENGINE_VERSION
  DB_BACKUP_RETENTION_DAYS = var.DB_BACKUP_RETENTION_DAYS
  PUBLICLY_ACCESSIBLE      = var.PUBLICLY_ACCESSIBLE

  # These must reference the main network module
  VPC_ID     = var.VPC_ID
  SUBNET_IDS = var.SUBNET_IDS
  RDS_SG_ID  = var.RDS_SG_ID
}

# Variables that must be provided when using this legacy module
variable "PROJECT_NAME" {
  description = "Project name for resource naming"
  type        = string
}

variable "DB_NAME" {
  description = "Database name"
  type        = string
}

variable "DB_USERNAME" {
  description = "Database username"
  type        = string
}

variable "DB_PASSWORD" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "DB_INSTANCE_CLASS" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "DB_ALLOCATED_STORAGE" {
  description = "Allocated storage in GB"
  type        = number
  default     = 20
}

variable "DB_ENGINE_VERSION" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "16.1"
}

variable "DB_BACKUP_RETENTION_DAYS" {
  description = "Backup retention period in days"
  type        = number
  default     = 7
}

variable "PUBLICLY_ACCESSIBLE" {
  description = "Whether the RDS instance should be publicly accessible"
  type        = bool
  default     = false
}

# Network references from main infrastructure
variable "VPC_ID" {
  description = "VPC ID from main infrastructure"
  type        = string
}

variable "SUBNET_IDS" {
  description = "Subnet IDs from main infrastructure"
  type        = list(string)
}

variable "RDS_SG_ID" {
  description = "RDS Security Group ID from main infrastructure"
  type        = string
}

# Outputs
output "db_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = module.database.db_endpoint
  sensitive   = true
}

output "db_port" {
  description = "RDS PostgreSQL port"
  value       = module.database.db_port
}
