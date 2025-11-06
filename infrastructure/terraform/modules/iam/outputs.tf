# Legacy ETL role outputs (for backward compatibility)
output "role_arn"   { value = aws_iam_role.etl_role.arn }
output "policy_arn" { value = aws_iam_policy.etl_policy.arn }

# New role outputs for Snowflake pipeline
output "kafka_connector_role_arn" {
  description = "ARN of the IAM role for Kafka Connect"
  value       = aws_iam_role.kafka_connect_role.arn
}

output "kafka_connector_policy_arn" {
  description = "ARN of the IAM policy for Kafka Connect"
  value       = aws_iam_policy.kafka_connect_policy.arn
}

output "spark_execution_role_arn" {
  description = "ARN of the IAM role for Spark/Databricks execution"
  value       = aws_iam_role.spark_execution_role.arn
}

output "spark_execution_policy_arn" {
  description = "ARN of the IAM policy for Spark/Databricks execution"
  value       = aws_iam_policy.spark_execution_policy.arn
}

output "snowflake_role_arn" {
  description = "ARN of the IAM role for Snowflake to assume"
  value       = aws_iam_role.snowflake_access_role.arn
}

output "snowflake_policy_arn" {
  description = "ARN of the IAM policy for Snowflake S3 access"
  value       = aws_iam_policy.snowflake_s3_silver_read.arn
}

# Summary of all created roles
output "created_roles" {
  description = "Summary of all IAM roles created"
  value = {
    etl_role             = aws_iam_role.etl_role.arn
    kafka_connect_role   = aws_iam_role.kafka_connect_role.arn
    spark_execution_role = aws_iam_role.spark_execution_role.arn
    snowflake_role       = aws_iam_role.snowflake_access_role.arn
  }
}
