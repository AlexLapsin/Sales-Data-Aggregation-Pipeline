terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.74"
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

# Snowflake provider (optional - only used when ENABLE_SNOWFLAKE_OBJECTS is true)
provider "snowflake" {
  account  = var.SNOWFLAKE_ACCOUNT
  user     = var.SNOWFLAKE_USER
  password = var.SNOWFLAKE_PASSWORD
  role     = var.SNOWFLAKE_ROLE
  region   = var.SNOWFLAKE_REGION
}
