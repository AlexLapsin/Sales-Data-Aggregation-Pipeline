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

# Schema for dbt test failures
resource "snowflake_schema" "tests" {
  database = snowflake_database.sales_dw.name
  name     = "TESTS"
  comment  = "Schema for dbt test failure storage"
}

# Warehouse for analytics and dbt transformations
# With Delta Direct zero-copy architecture, all Snowflake operations
# (dbt transformations, analytics queries) use this single warehouse
resource "snowflake_warehouse" "compute_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = "X-SMALL"  # Sufficient for Delta Direct + dbt transformations

  auto_suspend = 300  # 5 minutes
  auto_resume  = true

  initially_suspended = true
  comment = "Unified warehouse for analytics and dbt transformations with Delta Direct"
}

# Role for analytics/dbt
resource "snowflake_account_role" "analytics_role" {
  name    = "ANALYTICS_ROLE"
  comment = "Role for analytics and dbt transformations"
}

# Grant ANALYTICS_ROLE to the dbt/Airflow user
resource "snowflake_grant_account_role" "analytics_role_to_user" {
  role_name = snowflake_account_role.analytics_role.name
  user_name = var.SNOWFLAKE_USER
}

# Grant ownership of TESTS schema to ANALYTICS_ROLE
resource "snowflake_grant_ownership" "tests_schema_ownership" {
  account_role_name = snowflake_account_role.analytics_role.name
  on {
    object_type = "SCHEMA"
    object_name = "${snowflake_database.sales_dw.name}.${snowflake_schema.tests.name}"
  }
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

# Grant permissions on all future tables (excludes TESTS - handled by ownership)
resource "snowflake_grant_privileges_to_account_role" "analytics_future_tables" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.sales_dw.name}.${each.value}"
    }
  }
}

# Grant permissions on all future views (excludes TESTS - handled by ownership)
resource "snowflake_grant_privileges_to_account_role" "analytics_future_views" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["SELECT"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "${snowflake_database.sales_dw.name}.${each.value}"
    }
  }
}

# Grant permissions on ALL existing tables (excludes TESTS - handled by ownership)
resource "snowflake_grant_privileges_to_account_role" "analytics_all_tables" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.sales_dw.name}.${each.value}"
    }
  }
}

# Grant permissions on ALL existing views (excludes TESTS - handled by ownership)
resource "snowflake_grant_privileges_to_account_role" "analytics_all_views" {
  for_each = toset([
    snowflake_schema.raw.name,
    snowflake_schema.staging.name,
    snowflake_schema.marts.name
  ])

  privileges = ["SELECT"]
  account_role_name = snowflake_account_role.analytics_role.name
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = "${snowflake_database.sales_dw.name}.${each.value}"
    }
  }
}
