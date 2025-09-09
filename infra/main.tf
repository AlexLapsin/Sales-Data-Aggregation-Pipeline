module "network" {
  source       = "./modules/network"
  PROJECT_NAME = var.PROJECT_NAME
  ALLOWED_CIDR = var.ALLOWED_CIDR
}

module "storage" {
  source           = "./modules/storage"
  PROJECT_NAME     = var.PROJECT_NAME
  RAW_BUCKET       = var.RAW_BUCKET
  PROCESSED_BUCKET = var.PROCESSED_BUCKET
}

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

  VPC_ID     = module.network.vpc_id
  SUBNET_IDS = module.network.subnet_ids
  RDS_SG_ID  = module.network.rds_sg_id
}

module "iam" {
  source                = "./modules/iam"
  PROJECT_NAME          = var.PROJECT_NAME
  TRUSTED_PRINCIPAL_ARN = var.TRUSTED_PRINCIPAL_ARN
  RAW_BUCKET            = var.RAW_BUCKET
  PROCESSED_BUCKET      = var.PROCESSED_BUCKET
}

# Kafka module for streaming data pipeline
module "kafka" {
  source       = "./modules/kafka"
  count        = var.ENABLE_MSK ? 1 : 0

  PROJECT_NAME         = var.PROJECT_NAME
  ENVIRONMENT          = var.ENVIRONMENT
  KAFKA_VERSION        = var.KAFKA_VERSION
  BROKER_INSTANCE_TYPE = var.BROKER_INSTANCE_TYPE
  NUMBER_OF_BROKERS    = var.NUMBER_OF_BROKERS

  VPC_ID             = module.network.vpc_id
  SUBNET_IDS         = module.network.subnet_ids
  SECURITY_GROUP_IDS = [module.network.rds_sg_id] # Reuse existing SG for simplicity
}

# Optional Snowflake objects module (warehouses, databases, etc.)
module "snowflake" {
  source = "./modules/snowflake"
  count  = var.ENABLE_SNOWFLAKE_OBJECTS ? 1 : 0

  PROJECT_NAME      = var.PROJECT_NAME
  ENVIRONMENT       = var.ENVIRONMENT
  SNOWFLAKE_ACCOUNT = var.SNOWFLAKE_ACCOUNT
}
