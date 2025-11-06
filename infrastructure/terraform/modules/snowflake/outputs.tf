# infra/modules/snowflake/outputs.tf

output "database_name" {
  description = "Name of the Snowflake database"
  value       = snowflake_database.sales_dw.name
}

output "schemas" {
  description = "Created Snowflake schemas"
  value = {
    raw     = snowflake_schema.raw.name
    staging = snowflake_schema.staging.name
    marts   = snowflake_schema.marts.name
  }
}

output "warehouse" {
  description = "Snowflake warehouse for analytics and dbt (unified with Delta Direct)"
  value = snowflake_warehouse.compute_wh.name
}

output "analytics_role" {
  description = "Snowflake role for analytics and dbt transformations"
  value = snowflake_account_role.analytics_role.name
}

output "connection_info" {
  description = "Snowflake connection information"
  value = {
    database  = snowflake_database.sales_dw.name
    warehouse = snowflake_warehouse.compute_wh.name
    raw_schema = "${snowflake_database.sales_dw.name}.${snowflake_schema.raw.name}"
    staging_schema = "${snowflake_database.sales_dw.name}.${snowflake_schema.staging.name}"
    marts_schema = "${snowflake_database.sales_dw.name}.${snowflake_schema.marts.name}"
  }
}

output "delta_direct_resources" {
  description = "Delta Direct (Iceberg) resource names for zero-copy architecture"
  value = {
    external_volume     = "S3_SILVER_VOLUME"              # Created by Terraform
    catalog_integration = "DELTA_CATALOG"                 # Created by Python script
    iceberg_table       = "SALES_SILVER_EXTERNAL"         # Created by Python script
    full_table_name     = "${snowflake_database.sales_dw.name}.${snowflake_schema.raw.name}.SALES_SILVER_EXTERNAL"
  }
}

output "external_table_name" {
  description = "Fully qualified name of the Iceberg external table (created manually)"
  value       = "${snowflake_database.sales_dw.name}.${snowflake_schema.raw.name}.SALES_SILVER_EXTERNAL"
}
