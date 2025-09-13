# infra/modules/kafka/variables.tf

variable "PROJECT_NAME" {
  description = "Name of the project"
  type        = string
}

variable "ENVIRONMENT" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "KAFKA_VERSION" {
  description = "Kafka version for MSK cluster"
  type        = string
  default     = "2.8.1"
}

variable "BROKER_INSTANCE_TYPE" {
  description = "Instance type for MSK brokers"
  type        = string
  default     = "kafka.t3.small"
}

variable "NUMBER_OF_BROKERS" {
  description = "Number of broker nodes in MSK cluster"
  type        = number
  default     = 3
}

variable "VPC_ID" {
  description = "VPC ID where MSK cluster will be deployed"
  type        = string
}

variable "SUBNET_IDS" {
  description = "List of subnet IDs for MSK cluster"
  type        = list(string)
}

variable "SECURITY_GROUP_IDS" {
  description = "List of security group IDs for MSK cluster"
  type        = list(string)
}
