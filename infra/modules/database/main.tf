resource "aws_db_subnet_group" "this" {
  name       = "${var.PROJECT_NAME}-db-subnets"
  subnet_ids = var.SUBNET_IDS
  tags = {
    Name = "${var.PROJECT_NAME}-db-subnets"
  }
}

resource "aws_db_instance" "this" {
  identifier              = "${var.PROJECT_NAME}-db"
  db_name                 = var.DB_NAME
  engine                  = "postgres"
  engine_version          = var.DB_ENGINE_VERSION
  instance_class          = var.DB_INSTANCE_CLASS
  allocated_storage       = var.DB_ALLOCATED_STORAGE
  storage_type            = "gp2"

  username                = var.DB_USERNAME
  password                = var.DB_PASSWORD

  db_subnet_group_name    = aws_db_subnet_group.this.name
  vpc_security_group_ids  = [var.RDS_SG_ID]
  publicly_accessible     = var.PUBLICLY_ACCESSIBLE

  backup_retention_period = var.DB_BACKUP_RETENTION_DAYS
  skip_final_snapshot     = true

  tags = {
    Name = "${var.PROJECT_NAME}-db"
  }
}
