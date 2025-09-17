# infra/modules/kafka/main.tf
# AWS MSK (Managed Streaming for Apache Kafka) cluster

# MSK Cluster Configuration
resource "aws_msk_configuration" "sales_pipeline" {
  kafka_versions = [var.KAFKA_VERSION]
  name           = "${var.PROJECT_NAME}-${var.ENVIRONMENT}-msk-config"

  server_properties = <<PROPERTIES
auto.create.topics.enable=true
default.replication.factor=3
min.insync.replicas=2
num.partitions=3
log.retention.hours=168
log.segment.bytes=1073741824
PROPERTIES
}

# MSK Cluster
resource "aws_msk_cluster" "sales_pipeline" {
  cluster_name           = "${var.PROJECT_NAME}-${var.ENVIRONMENT}-cluster"
  kafka_version          = var.KAFKA_VERSION
  number_of_broker_nodes = var.NUMBER_OF_BROKERS

  broker_node_group_info {
    instance_type   = var.BROKER_INSTANCE_TYPE
    client_subnets  = var.SUBNET_IDS
    security_groups = var.SECURITY_GROUP_IDS

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.sales_pipeline.arn
    revision = aws_msk_configuration.sales_pipeline.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = {
    Name        = "${var.PROJECT_NAME}-${var.ENVIRONMENT}-msk-cluster"
    Environment = var.ENVIRONMENT
    Project     = var.PROJECT_NAME
  }
}

# KMS Key for MSK encryption
resource "aws_kms_key" "msk" {
  description             = "KMS key for MSK cluster encryption"
  deletion_window_in_days = 7

  tags = {
    Name        = "${var.PROJECT_NAME}-${var.ENVIRONMENT}-msk-key"
    Environment = var.ENVIRONMENT
    Project     = var.PROJECT_NAME
  }
}

resource "aws_kms_alias" "msk" {
  name          = "alias/${var.PROJECT_NAME}-${var.ENVIRONMENT}-msk"
  target_key_id = aws_kms_key.msk.key_id
}

# CloudWatch Log Group for MSK
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.PROJECT_NAME}-${var.ENVIRONMENT}"
  retention_in_days = 7

  tags = {
    Name        = "${var.PROJECT_NAME}-${var.ENVIRONMENT}-msk-logs"
    Environment = var.ENVIRONMENT
    Project     = var.PROJECT_NAME
  }
}

# Additional security group rules for MSK (if needed)
resource "aws_security_group_rule" "msk_kafka_port" {
  count           = length(var.SECURITY_GROUP_IDS)
  type            = "ingress"
  from_port       = 9092
  to_port         = 9094
  protocol        = "tcp"
  cidr_blocks     = ["10.0.0.0/8"]  # Adjust based on your VPC CIDR
  security_group_id = var.SECURITY_GROUP_IDS[count.index]
  description     = "Kafka broker ports for MSK"
}

resource "aws_security_group_rule" "msk_zookeeper_port" {
  count           = length(var.SECURITY_GROUP_IDS)
  type            = "ingress"
  from_port       = 2181
  to_port         = 2181
  protocol        = "tcp"
  cidr_blocks     = ["10.0.0.0/8"]  # Adjust based on your VPC CIDR
  security_group_id = var.SECURITY_GROUP_IDS[count.index]
  description     = "Zookeeper port for MSK"
}
