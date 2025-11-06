# Deploy Infrastructure

Deploy AWS and Snowflake infrastructure using Terraform.

## Prerequisites

- AWS account with admin access
- AWS CLI configured with credentials
- Snowflake account (trial or production)
- Terraform 1.0+ installed
- `.env` file configured with credentials

## Snowflake Role Setup (One-Time)

Before using Terraform, create a dedicated infrastructure automation role in Snowflake. This follows security best practices by avoiding ACCOUNTADMIN for automation.

**Run these commands once in Snowflake Web UI:**

1. Navigate to https://app.snowflake.com
2. Login and open a new worksheet
3. Execute the following SQL as ACCOUNTADMIN:

```sql
USE ROLE ACCOUNTADMIN;

-- Create infrastructure automation role
CREATE ROLE IF NOT EXISTS TERRAFORM_ROLE
  COMMENT = 'Infrastructure automation role for Terraform (least privilege)';

-- Grant account-level privileges for infrastructure management
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TERRAFORM_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TERRAFORM_ROLE;
GRANT CREATE ROLE ON ACCOUNT TO ROLE TERRAFORM_ROLE;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE TERRAFORM_ROLE;
GRANT CREATE USER ON ACCOUNT TO ROLE TERRAFORM_ROLE;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE TERRAFORM_ROLE;
GRANT CREATE EXTERNAL VOLUME ON ACCOUNT TO ROLE TERRAFORM_ROLE;

-- Grant to your Terraform user (replace USERNAME with your snowflake username)
GRANT ROLE TERRAFORM_ROLE TO USER USERNAME;

-- Establish role hierarchy (best practice)
GRANT ROLE TERRAFORM_ROLE TO ROLE SYSADMIN;

-- Verify setup
SHOW GRANTS TO ROLE TERRAFORM_ROLE;
```

**Update your `.env` file to use TERRAFORM_ROLE:**

```bash
SNOWFLAKE_ROLE=TERRAFORM_ROLE
```

**Why this matters:**
- Follows Snowflake security best practices
- Least privilege principle (only infrastructure permissions)
- Avoids using ACCOUNTADMIN for automation
- No manual role switching required

Reference: [Snowflake Access Control Best Practices](https://docs.snowflake.com/en/user-guide/security-access-control-considerations)

## Export Environment Variables

Terraform reads configuration from environment variables. Export them:

```bash
source export_tf_vars.sh
```

Verify variables are set:

```bash
env | grep TF_VAR
```

Expected output:
```
TF_VAR_RAW_BUCKET=your-project-raw-bucket
TF_VAR_PROCESSED_BUCKET=your-project-processed-bucket
TF_VAR_SNOWFLAKE_ACCOUNT_NAME=your_account
TF_VAR_SNOWFLAKE_USER=your_username
...
```

## Initialize Terraform

Navigate to Terraform directory:

```bash
cd infrastructure/terraform
```

Initialize Terraform providers and modules:

```bash
terraform init
```

Expected output:
```
Initializing modules...
Initializing provider plugins...
- Finding hashicorp/aws latest version...
- Finding snowflake-labs/snowflake latest version...

Terraform has been successfully initialized!
```

## Review Infrastructure Plan

Generate an execution plan to review changes:

```bash
terraform plan -out=tfplan
```

Review the output for:

**AWS Resources:**
- `aws_s3_bucket.raw_bucket` - Bronze layer storage
- `aws_s3_bucket.processed_bucket` - Silver layer storage
- `aws_iam_role.snowflake_role` - Snowflake access to S3
- `aws_iam_policy.snowflake_s3_policy` - S3 read/write permissions

**Snowflake Resources:**
- `snowflake_database.sales_db` - Main database
- `snowflake_schema.public` - Public schema
- `snowflake_warehouse.compute_wh` - Compute warehouse
- `snowflake_storage_integration.s3_integration` - S3 access integration

Expected summary:
```
Plan: 12 to add, 0 to change, 0 to destroy.
```

## Deploy Infrastructure

Apply the Terraform plan:

```bash
terraform apply tfplan
```

Deployment takes 5-10 minutes. Monitor progress:
- S3 buckets: 1-2 minutes
- IAM roles: 1-2 minutes
- Snowflake resources: 3-5 minutes

Expected output:
```
Apply complete! Resources: 12 added, 0 changed, 0 destroyed.

Outputs:

processed_bucket_name = "your-project-processed-bucket"
raw_bucket_name = "your-project-raw-bucket"
snowflake_database = "SALES_DB"
snowflake_integration_external_id = "ABC123_SFCRole=..."
snowflake_integration_user_arn = "arn:aws:iam::123456789012:user/..."
snowflake_schema = "PUBLIC"
snowflake_warehouse = "COMPUTE_WH"
```

## Verify Deployment

Check S3 buckets exist:

```bash
aws s3 ls | grep your-project
```

Expected output:
```
2024-01-15 10:30:00 your-project-processed-bucket
2024-01-15 10:30:00 your-project-raw-bucket
```

Check IAM role:

```bash
aws iam get-role --role-name snowflake-s3-access-role
```

Expected output: JSON with role details and trust policy

Verify Snowflake database:

```sql
-- Connect to Snowflake using SnowSQL or UI
SHOW DATABASES LIKE 'SALES_DB';
USE DATABASE SALES_DB;
SHOW SCHEMAS;
```

Expected output:
```
SALES_DB | PUBLIC | ...
```

## Configure Snowflake Storage Integration

The storage integration allows Snowflake to access S3. Retrieve the integration details:

```bash
terraform output snowflake_integration_user_arn
terraform output snowflake_integration_external_id
```

In Snowflake, verify the integration:

```sql
DESC STORAGE INTEGRATION s3_integration;
```

Expected fields:
- `STORAGE_AWS_IAM_USER_ARN` - ARN for trust policy
- `STORAGE_AWS_EXTERNAL_ID` - External ID for trust policy
- `ENABLED` - TRUE

The Terraform configuration automatically updates the IAM trust policy with these values.

## Update Environment File

After deployment, update `.env` with the actual resource names:

```bash
# Update these values in .env
RAW_BUCKET=your-project-raw-bucket
PROCESSED_BUCKET=your-project-processed-bucket
SNOWFLAKE_DATABASE=SALES_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

## Terraform State Management

Terraform maintains state in `terraform.tfstate`. This file tracks deployed resources.

**Important:**
- Do not delete `terraform.tfstate` - it tracks your infrastructure
- Do not commit `terraform.tfstate` to git - contains sensitive data
- For team environments, use remote state (S3 + DynamoDB)

Configure remote state (optional):

```hcl
# Add to providers.tf
terraform {
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "sales-pipeline/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}
```

## Destroy Infrastructure

To tear down all resources:

```bash
terraform destroy
```

Type `yes` when prompted. This removes:
- All S3 buckets and contents
- IAM roles and policies
- Snowflake database, schema, warehouse

**Warning:** This is irreversible. Export any data you need before destroying.

## Troubleshooting

**Error: "InvalidAccessKeyId"**
- Verify AWS credentials: `aws sts get-caller-identity`
- Check `.env` has correct AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

**Error: "Snowflake connection failed"**
- Verify credentials in `.env`
- Test connection: `snowsql -a your_account -u your_username`
- Check account name format: `account.region` (e.g., `xy12345.us-east-1`)

**Error: "BucketAlreadyExists"**
- Bucket names must be globally unique
- Change RAW_BUCKET and PROCESSED_BUCKET in `.env`
- Re-run `source export_tf_vars.sh`

**Error: "InsufficientPrivileges" in Snowflake**
- Verify you completed the Snowflake Role Setup section
- Confirm SNOWFLAKE_ROLE=TERRAFORM_ROLE in your `.env` file
- Run role setup SQL in Snowflake Web UI (see setup section above)

## Next Steps

- [Configure Environment](configure-environment.md) - Set up Docker and local services
- [Upload Sample Data](run-batch-pipeline.md#upload-data) - Add CSV files to Bronze layer
