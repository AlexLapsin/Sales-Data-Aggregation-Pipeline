output "raw_bucket"        { value = module.storage.raw_bucket_name }
output "processed_bucket"  { value = module.storage.processed_bucket_name }
output "db_endpoint"       { value = module.database.db_endpoint }
output "db_port"           { value = module.database.db_port }
output "etl_role_arn"      { value = module.iam.role_arn }
output "etl_policy_arn"    { value = module.iam.policy_arn }
