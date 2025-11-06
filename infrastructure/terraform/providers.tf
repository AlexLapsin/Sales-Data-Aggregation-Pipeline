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
  # Using host parameter to specify full account locator URL
  # This works more reliably than account_name + organization_name in v2.x
  host              = "${var.SNOWFLAKE_ORGANIZATION_NAME}-${var.SNOWFLAKE_ACCOUNT_NAME}.snowflakecomputing.com"
  user              = var.SNOWFLAKE_USER

  # Explicitly set authenticator for keypair authentication
  authenticator     = "SNOWFLAKE_JWT"

  # Keypair authentication (recommended - more secure than password)
  private_key       = file(var.SNOWFLAKE_PRIVATE_KEY_PATH)
  private_key_passphrase = var.SNOWFLAKE_KEY_PASSPHRASE

  role              = var.SNOWFLAKE_ROLE

  # Don't specify warehouse in provider config to avoid chicken-and-egg problem
  # Individual resources will specify warehouse as needed
  warehouse = ""

  # Enable preview features for Delta Direct (External Volume)
  preview_features_enabled = ["snowflake_external_volume_resource"]
}

# Alternative provider configuration without warehouse for bootstrap operations
provider "snowflake" {
  alias             = "bootstrap"
  # Using host parameter to specify full account locator URL
  host              = "${var.SNOWFLAKE_ORGANIZATION_NAME}-${var.SNOWFLAKE_ACCOUNT_NAME}.snowflakecomputing.com"
  user              = var.SNOWFLAKE_USER

  # Explicitly set authenticator for keypair authentication
  authenticator     = "SNOWFLAKE_JWT"

  # Keypair authentication (recommended - more secure than password)
  private_key       = file(var.SNOWFLAKE_PRIVATE_KEY_PATH)
  private_key_passphrase = var.SNOWFLAKE_KEY_PASSPHRASE

  role              = var.SNOWFLAKE_ROLE

  # Enable preview features for Delta Direct (External Volume)
  preview_features_enabled = ["snowflake_external_volume_resource"]

  # No warehouse specified for bootstrap operations
}
