# ============================================================================
# Delta Direct (Iceberg) Infrastructure for Snowflake
# ============================================================================
#
# TERRAFORM PROVIDER STATUS (January 2025):
#
# SUPPORTED (Managed by Terraform):
# - snowflake_external_volume (added in v0.90.0)
# - AWS IAM role for Snowflake S3 access
# - S3 bucket permissions
#
# NOT SUPPORTED (Managed by Python Script):
# - snowflake_catalog_integration (GitHub issue #2249, opened Dec 2023)
# - snowflake_iceberg_table (GitHub issue #2249, opened Dec 2023)
#
# DEPLOYMENT WORKFLOW:
# 1. Run: terraform apply (creates IAM role + External Volume)
# 2. Run: python deploy/snowflake/setup_delta_direct.py (creates Catalog + Iceberg Table)
#
# See: deploy/snowflake/README.md for detailed deployment instructions
# See: docs/architecture/automation_research.md for research on this approach
# See: explanation_deploy.md for technical explanation of Delta Direct
#
# ============================================================================

# External Volume for S3 Silver Layer (Delta Lake)
# Zero-copy architecture: Snowflake queries Delta Lake in S3 without copying data
# Note: allow_writes required for Iceberg table metadata, but data queries are zero-copy
resource "snowflake_external_volume" "s3_silver_volume" {
  count = var.ENABLE_SNOWFLAKE_OBJECTS ? 1 : 0

  name         = "S3_SILVER_VOLUME"
  comment      = "External volume for Delta Direct zero-copy queries of Delta Lake Silver layer"
  allow_writes = "true"  # Required for Iceberg metadata writes

  storage_location {
    storage_location_name = "silver-delta-storage"
    storage_provider      = "S3"
    storage_base_url      = "s3://${var.PROCESSED_BUCKET}/silver/"
    storage_aws_role_arn  = var.snowflake_iam_role_arn
  }
}

# Output for Python deployment script
output "external_volume_name" {
  description = "External Volume created by Terraform (used by deploy/snowflake/setup_delta_direct.py)"
  value       = var.ENABLE_SNOWFLAKE_OBJECTS ? snowflake_external_volume.s3_silver_volume[0].name : null
}

output "snowflake_iam_role_arn" {
  description = "IAM role ARN for Snowflake S3 access (used by External Volume)"
  value       = var.snowflake_iam_role_arn
}

output "external_volume_external_id" {
  description = "Snowflake-generated External ID - MUST match IAM role trust policy in .env"
  value       = var.ENABLE_SNOWFLAKE_OBJECTS ? snowflake_external_volume.s3_silver_volume[0].storage_location[0].storage_aws_external_id : null
  sensitive   = true
}
