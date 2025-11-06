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

module "iam" {
  source                = "./modules/iam"
  PROJECT_NAME          = var.PROJECT_NAME
  ENVIRONMENT           = var.ENVIRONMENT
  AWS_REGION            = var.AWS_REGION
  TRUSTED_PRINCIPAL_ARN = var.TRUSTED_PRINCIPAL_ARN
  RAW_BUCKET            = var.RAW_BUCKET
  PROCESSED_BUCKET      = var.PROCESSED_BUCKET
  SNOWFLAKE_IAM_USER_ARN = var.SNOWFLAKE_IAM_USER_ARN
  SNOWFLAKE_EXTERNAL_ID  = var.SNOWFLAKE_EXTERNAL_ID
}

# Optional AWS MSK (Managed Streaming for Kafka) module
# By default, Kafka runs locally via Docker (recommended for development)
# Set ENABLE_MSK=true in .env if you need managed Kafka in AWS
module "kafka" {
  source = "./modules/kafka"
  count  = var.ENABLE_MSK ? 1 : 0

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
# Enable with ENABLE_SNOWFLAKE_OBJECTS=true in .env
module "snowflake" {
  source = "./modules/snowflake"
  count  = var.ENABLE_SNOWFLAKE_OBJECTS ? 1 : 0

  PROJECT_NAME              = var.PROJECT_NAME
  ENVIRONMENT               = var.ENVIRONMENT
  PROCESSED_BUCKET          = var.PROCESSED_BUCKET
  snowflake_iam_role_arn    = module.iam.snowflake_role_arn
  ENABLE_SNOWFLAKE_OBJECTS  = var.ENABLE_SNOWFLAKE_OBJECTS
  SNOWFLAKE_EXTERNAL_ID     = var.SNOWFLAKE_EXTERNAL_ID
  SNOWFLAKE_USER            = var.SNOWFLAKE_USER
}
