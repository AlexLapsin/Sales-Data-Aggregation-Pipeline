terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 2.7.0"
    }
  }
}

provider "aws" {
  region = var.AWS_REGION
  default_tags {
    tags = {
      Project     = var.PROJECT_NAME
      Environment = var.ENVIRONMENT
      ManagedBy   = "Terraform"
    }
  }
}

provider "snowflake" {
  account_name      = var.SNOWFLAKE_ACCOUNT_NAME
  organization_name = var.SNOWFLAKE_ORGANIZATION_NAME
  user              = var.SNOWFLAKE_USER
  password          = var.SNOWFLAKE_PASSWORD
  role              = var.SNOWFLAKE_ROLE

  # Don't specify warehouse in provider config to avoid chicken-and-egg problem
  # Individual resources will specify warehouse as needed
  warehouse = ""
}

# Alternative provider configuration without warehouse for bootstrap operations
provider "snowflake" {
  alias             = "bootstrap"
  account_name      = var.SNOWFLAKE_ACCOUNT_NAME
  organization_name = var.SNOWFLAKE_ORGANIZATION_NAME
  user              = var.SNOWFLAKE_USER
  password          = var.SNOWFLAKE_PASSWORD
  role              = var.SNOWFLAKE_ROLE
  # No warehouse specified for bootstrap operations
}
