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

output "warehouses" {
  description = "Created Snowflake warehouses"
  value = {
    compute = snowflake_warehouse.compute_wh.name
    etl     = snowflake_warehouse.etl_wh.name
  }
}

output "roles" {
  description = "Created Snowflake roles"
  value = {
    kafka_connector = snowflake_account_role.kafka_role.name
    analytics       = snowflake_account_role.analytics_role.name
  }
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
