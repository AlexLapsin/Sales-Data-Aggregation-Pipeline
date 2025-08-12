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
