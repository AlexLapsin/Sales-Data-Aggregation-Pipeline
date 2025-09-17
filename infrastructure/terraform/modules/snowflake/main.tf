# infra/modules/snowflake/main.tf

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 2.7.0"
    }
  }
}

# Database for the sales data warehouse
resource "snowflake_database" "sales_dw" {
  name    = "SALES_DW"
  comment = "Sales data warehouse for ${var.PROJECT_NAME}"

  data_retention_time_in_days = 1  # Maximum allowed for trial accounts
}

# Schema for raw data
resource "snowflake_schema" "raw" {
  database = snowflake_database.sales_dw.name
  name     = "RAW"
  comment  = "Raw data ingested from various sources"
}

# Schema for staging/transformed data
resource "snowflake_schema" "staging" {
  database = snowflake_database.sales_dw.name
  name     = "STAGING"
  comment  = "Staging area for data transformations"
}

# Schema for analytics/marts
resource "snowflake_schema" "marts" {
  database = snowflake_database.sales_dw.name
  name     = "MARTS"
  comment  = "Analytics-ready data marts"
}

# Warehouse for compute
resource "snowflake_warehouse" "compute_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = "X-SMALL"  # Start small for development

  auto_suspend = 300  # 5 minutes
  auto_resume  = true

  initially_suspended = true
  comment = "General compute warehouse for ${var.PROJECT_NAME}"
}

# Warehouse for ETL operations
resource "snowflake_warehouse" "etl_wh" {
  name           = "ETL_WH"
  warehouse_size = "SMALL"    # Slightly larger for ETL

  auto_suspend = 300  # 5 minutes
  auto_resume  = true

  initially_suspended = true
  comment = "Dedicated warehouse for ETL operations"
}

# Role for Kafka Connect
resource "snowflake_account_role" "kafka_role" {
  name    = "KAFKA_CONNECTOR_ROLE"
  comment = "Role for Kafka Connect to load data"
}

# Grant permissions to Kafka role
resource "snowflake_grant_privileges_to_account_role" "kafka_database_usage" {
  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.kafka_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.sales_dw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "kafka_schema_usage" {
  for_each = toset([snowflake_schema.raw.name, snowflake_schema.staging.name])

  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.kafka_role.name
  on_schema {
    schema_name = "${snowflake_database.sales_dw.name}.${each.value}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "kafka_schema_create_table" {
  for_each = toset([snowflake_schema.raw.name, snowflake_schema.staging.name])

  privileges = ["CREATE TABLE"]
  account_role_name = snowflake_account_role.kafka_role.name
  on_schema {
    schema_name = "${snowflake_database.sales_dw.name}.${each.value}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "kafka_warehouse_usage" {
  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.kafka_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.etl_wh.name
  }
}

# Role for analytics/dbt
resource "snowflake_account_role" "analytics_role" {
  name    = "ANALYTICS_ROLE"
  comment = "Role for analytics and dbt transformations"
}

# Grant permissions to analytics role
resource "snowflake_grant_privileges_to_account_role" "analytics_database_usage" {
  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.sales_dw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analytics_schema_usage" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema {
    schema_name = "${snowflake_database.sales_dw.name}.${each.value}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "analytics_schema_create_table" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["CREATE TABLE"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema {
    schema_name = "${snowflake_database.sales_dw.name}.${each.value}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "analytics_schema_create_view" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["CREATE VIEW"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema {
    schema_name = "${snowflake_database.sales_dw.name}.${each.value}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "analytics_warehouse_usage" {
  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.compute_wh.name
  }
}
