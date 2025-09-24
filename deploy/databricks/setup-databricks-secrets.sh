#!/bin/bash
# Databricks Secrets Setup Script
# Automates creation of Databricks secrets from environment variables
# Usage: ./scripts/setup-databricks-secrets.sh

set -e  # Exit on any error

echo "Setting up Databricks secrets from environment variables..."
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found in current directory"
    echo "Please ensure you're running this from the project root"
    exit 1
fi

# Source environment variables
echo "Loading environment variables from .env..."
source .env

# Verify required variables are set
required_vars=("DATABRICKS_HOST" "DATABRICKS_TOKEN" "SNOWFLAKE_ACCOUNT_NAME" "SNOWFLAKE_ORGANIZATION_NAME" "SNOWFLAKE_USER" "SNOWFLAKE_PASSWORD" "SNOWFLAKE_WAREHOUSE" "SNOWFLAKE_DATABASE" "SNOWFLAKE_SCHEMA" "SNOWFLAKE_ROLE" "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY")

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: Required environment variable $var is not set"
        exit 1
    fi
done

echo "All required environment variables found"
echo ""

# Set Databricks environment variables for authentication
export DATABRICKS_HOST="$DATABRICKS_HOST"
export DATABRICKS_TOKEN="$DATABRICKS_TOKEN"
echo "Databricks CLI configured with environment variables"

echo ""

# Create secret scope
echo "Creating secret scope 'snowflake-secrets'..."
if databricks secrets create-scope snowflake-secrets 2>/dev/null; then
    echo "Secret scope 'snowflake-secrets' created successfully"
else
    echo "Secret scope 'snowflake-secrets' already exists"
fi

echo ""

# Combine account name and organization for full account identifier
SNOWFLAKE_FULL_ACCOUNT="${SNOWFLAKE_ACCOUNT_NAME}.${SNOWFLAKE_ORGANIZATION_NAME}"

# Create secrets
echo "Creating Snowflake secrets..."

echo "Creating snowflake-account..."
echo "$SNOWFLAKE_FULL_ACCOUNT" | databricks secrets put-secret snowflake-secrets snowflake-account

echo "Creating snowflake-user..."
echo "$SNOWFLAKE_USER" | databricks secrets put-secret snowflake-secrets snowflake-user

echo "Creating snowflake-password..."
echo "$SNOWFLAKE_PASSWORD" | databricks secrets put-secret snowflake-secrets snowflake-password

echo "Creating snowflake-warehouse..."
echo "$SNOWFLAKE_WAREHOUSE" | databricks secrets put-secret snowflake-secrets snowflake-warehouse

echo "Creating snowflake-database..."
echo "$SNOWFLAKE_DATABASE" | databricks secrets put-secret snowflake-secrets snowflake-database

echo "Creating snowflake-schema..."
echo "$SNOWFLAKE_SCHEMA" | databricks secrets put-secret snowflake-secrets snowflake-schema

echo "Creating snowflake-role..."
echo "$SNOWFLAKE_ROLE" | databricks secrets put-secret snowflake-secrets snowflake-role

echo "Creating aws-access-key-id..."
echo "$AWS_ACCESS_KEY_ID" | databricks secrets put-secret snowflake-secrets aws-access-key-id

echo "Creating aws-secret-access-key..."
echo "$AWS_SECRET_ACCESS_KEY" | databricks secrets put-secret snowflake-secrets aws-secret-access-key

echo ""

# Verify secrets were created
echo "Verifying secrets were created..."
if databricks secrets list-secrets snowflake-secrets > /dev/null 2>&1; then
    echo "All secrets created successfully"
    echo ""
    echo "Available secrets in 'snowflake-secrets' scope:"
    databricks secrets list-secrets snowflake-secrets
else
    echo "Error: Could not verify secrets creation"
    exit 1
fi

echo ""
echo "Databricks secrets setup completed successfully"
echo ""
echo "Next steps:"
echo "1. Update your ETL script to use dbutils.secrets.get()"
echo "2. Test the Databricks job execution"
echo ""
echo "Example usage in Python:"
echo "   snowflake_account = dbutils.secrets.get('snowflake-secrets', 'snowflake-account')"
