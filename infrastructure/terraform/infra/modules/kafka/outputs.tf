# infra/modules/kafka/outputs.tf

output "cluster_arn" {
  description = "ARN of the MSK cluster"
  value       = aws_msk_cluster.sales_pipeline.arn
}

output "cluster_name" {
  description = "Name of the MSK cluster"
  value       = aws_msk_cluster.sales_pipeline.cluster_name
}

output "bootstrap_brokers" {
  description = "Bootstrap broker endpoints for the MSK cluster"
  value       = aws_msk_cluster.sales_pipeline.bootstrap_brokers
}

output "bootstrap_brokers_tls" {
  description = "TLS bootstrap broker endpoints for the MSK cluster"
  value       = aws_msk_cluster.sales_pipeline.bootstrap_brokers_tls
}

output "zookeeper_connect_string" {
  description = "Zookeeper connection string for the MSK cluster"
  value       = aws_msk_cluster.sales_pipeline.zookeeper_connect_string
}

output "kafka_version" {
  description = "Version of Kafka running on the cluster"
  value       = aws_msk_cluster.sales_pipeline.kafka_version
}

output "kms_key_id" {
  description = "KMS key ID used for encryption"
  value       = aws_kms_key.msk.key_id
}

output "log_group_name" {
  description = "CloudWatch log group name for MSK logs"
  value       = aws_cloudwatch_log_group.msk.name
}
