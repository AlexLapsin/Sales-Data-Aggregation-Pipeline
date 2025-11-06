# Snowflake Delta Direct Deployment

Automated deployment script for Snowflake Delta Direct (Iceberg Tables) resources that are not yet supported in the Terraform Snowflake provider.

## Purpose

This script automates creation and teardown of Snowflake resources required for reading Delta Lake tables from S3 Silver layer:

- **Catalog Integration**: Translates Delta Lake transaction logs for Snowflake
- **Iceberg Table**: Virtual table in Snowflake that queries S3 data without copying
- **AUTO_REFRESH**: Automatic detection of new Delta Lake transactions
- **Permissions**: Grant SELECT access to analytics roles

These resources enable zero-copy architecture where data stays in S3 and Snowflake queries it in place.

## Prerequisites

### Infrastructure (Must Complete First)

1. **Terraform Infrastructure Deployed**
   ```bash
   cd infrastructure/terraform
   terraform init
   terraform apply
   ```

   This creates:
   - AWS IAM role for Snowflake S3 access
   - S3 buckets (Bronze/Silver layers)
   - Snowflake External Volume (S3_SILVER_VOLUME)
   - Snowflake roles and warehouses

2. **Silver Layer Data Available** (Optional)
   - Delta Lake tables written by Spark processing
   - Located in S3 processed bucket under `sales/` prefix
   - Script will work without data but verification will show 0 rows

### Snowflake Account

- Active Snowflake account (trial or enterprise)
- ACCOUNTADMIN privileges for creating integrations
- Delta Direct available on trial accounts (confirmed January 2025)

### Python Environment

- Python 3.8 or higher
- Dependencies from requirements/cloud.txt

## Installation

Install required Python packages:

```bash
pip install -r ../../requirements/cloud.txt
```

Key dependencies:
- `snowflake-connector-python>=3.0.0` - Snowflake database connector
- `snowflake>=0.8.0` - Snowflake Python API for catalog integrations and Iceberg tables
- `python-dotenv` - Environment variable management

## Configuration

### Environment Variables

Create or update `.env` file in project root:

```bash
# Snowflake Connection (Required)
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

# Snowflake Resources (Optional - defaults shown)
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SALES_DW
```

**Finding Your Account Identifier**:
- Format: `organization-account` (e.g., `myorg-myaccount`)
- Found in Snowflake UI: Click account name in bottom left
- Or from URL: `https://app.snowflake.com/myorg/myaccount/`

### Resource Names

Default resource names (configured in setup_delta_direct.py):
- Catalog Integration: `DELTA_CATALOG`
- Iceberg Table: `SALES_SILVER_EXTERNAL`
- External Volume: `S3_SILVER_VOLUME` (created by Terraform)
- Schema: `SALES_DW.RAW`
- Analytics Role: `ANALYTICS_ROLE`

## Usage

### Create Resources

Deploy all Delta Direct resources:

```bash
python setup_delta_direct.py
```

Output:
```
================================================================================
SNOWFLAKE DELTA DIRECT SETUP
================================================================================
Mode: CREATE

Loaded environment from: F:\GITHUB\sales_data_aggregation_pipeline\.env
Connecting to Snowflake account: myorg-myaccount
Using role: ACCOUNTADMIN, warehouse: COMPUTE_WH

[Validation] Checking prerequisites...
  [OK] External volume 'S3_SILVER_VOLUME' exists
  [OK] Role 'ANALYTICS_ROLE' exists
[Validation] Prerequisites validated successfully

[1/3] Creating catalog integration 'DELTA_CATALOG'...
  [SUCCESS] Catalog integration 'DELTA_CATALOG' created

[2/3] Creating Iceberg table 'SALES_SILVER_EXTERNAL'...
  [SUCCESS] Iceberg table 'SALES_SILVER_EXTERNAL' created with AUTO_REFRESH

[3/3] Granting permissions to 'ANALYTICS_ROLE'...
  [SUCCESS] Granted SELECT permission to 'ANALYTICS_ROLE'

[Verification] Checking deployed resources...
  [OK] Iceberg table contains 1,234 rows
  [OK] Iceberg table 'SALES_SILVER_EXTERNAL' is active

================================================================================
DELTA DIRECT SETUP COMPLETED SUCCESSFULLY
================================================================================

Next steps:
  1. Verify: SELECT COUNT(*) FROM SALES_DW.RAW.SALES_SILVER_EXTERNAL;
  2. Update dbt sources to reference SALES_SILVER_EXTERNAL
  3. Run dbt: cd dbt && dbt run
```

### Destroy Resources

Remove all Delta Direct resources:

```bash
python setup_delta_direct.py destroy
```

Resources are removed in reverse order:
1. Revoke permissions from ANALYTICS_ROLE
2. Drop Iceberg table
3. Drop catalog integration

**Note**: External Volume (S3_SILVER_VOLUME) is NOT destroyed - it's managed by Terraform.

### Idempotency

Script is idempotent and safe to run multiple times:
- If resources exist: Reports "[INFO] Resource already exists (idempotent)"
- If resources missing: Creates them
- No errors on repeated execution

## Verification

### Manual Verification in Snowflake

```sql
-- Check catalog integration
SHOW CATALOG INTEGRATIONS LIKE 'DELTA_CATALOG';

-- Check Iceberg table
SHOW ICEBERG TABLES LIKE 'SALES_SILVER_EXTERNAL' IN SCHEMA SALES_DW.RAW;

-- Verify data accessibility
SELECT COUNT(*) FROM SALES_DW.RAW.SALES_SILVER_EXTERNAL;

-- Check AUTO_REFRESH status
DESCRIBE ICEBERG TABLE SALES_DW.RAW.SALES_SILVER_EXTERNAL;

-- Sample data
SELECT * FROM SALES_DW.RAW.SALES_SILVER_EXTERNAL LIMIT 10;
```

### Expected Results

If Silver layer has data:
- Row count matches Delta Lake table in S3
- All 26 columns visible (row_id through processing_timestamp)
- AUTO_REFRESH enabled (new Delta transactions detected automatically)

If Silver layer is empty:
- Row count returns 0
- Table structure visible
- Ready to receive data from Spark processing

## Troubleshooting

### Error: Missing Required Environment Variables

```
ValueError: Missing required environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_PASSWORD
```

**Solution**: Ensure `.env` file contains all required variables.

### Error: External Volume Not Found

```
RuntimeError: External volume S3_SILVER_VOLUME not found.
Please run 'terraform apply' first to create AWS infrastructure.
```

**Solution**: Deploy Terraform infrastructure first:
```bash
cd infrastructure/terraform
source ../../export_tf_vars.sh
terraform apply
```

### Error: Permission Denied

```
ProgrammingError: Insufficient privileges to operate on integration 'DELTA_CATALOG'
```

**Solution**:
- Use ACCOUNTADMIN role (required for catalog integrations)
- Set `SNOWFLAKE_ROLE=ACCOUNTADMIN` in .env

### Warning: Iceberg Table Contains 0 Rows

```
[OK] Iceberg table contains 0 rows
```

**Not an Error**: This is normal if:
- Silver layer hasn't been populated yet
- Spark processing hasn't written Delta Lake tables
- S3 bucket is empty

**Next Steps**: Run batch processing pipeline to populate Silver layer.

### Error: Catalog Integration Already Exists

```
[INFO] Catalog integration 'DELTA_CATALOG' already exists (idempotent)
```

**Not an Error**: Script detected existing resource. This is normal behavior.

### Connection Timeout

**Symptoms**: Script hangs during session creation.

**Solution**:
- Check network connectivity to Snowflake
- Verify firewall rules allow outbound HTTPS
- Confirm credentials are correct
- Try shorter account identifier format

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy Snowflake Delta Direct

on:
  workflow_dispatch:
  push:
    branches: [main]
    paths:
      - 'infrastructure/terraform/**'
      - 'deploy/snowflake/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -r requirements/cloud.txt

      - name: Deploy Terraform (prerequisite)
        working-directory: infrastructure/terraform
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        run: |
          terraform init
          terraform apply -auto-approve

      - name: Deploy Delta Direct
        working-directory: deploy/snowflake
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ACCOUNTADMIN
          SNOWFLAKE_WAREHOUSE: COMPUTE_WH
          SNOWFLAKE_DATABASE: SALES_DW
        run: |
          python setup_delta_direct.py
```

### Deployment Order

Critical: Terraform MUST complete before running Python script.

```bash
# Correct order
terraform apply    # Creates External Volume
python setup_delta_direct.py  # Creates Catalog Integration + Iceberg Table

# Incorrect order (will fail)
python setup_delta_direct.py  # ERROR: External Volume not found
terraform apply
```

### Secret Management

Store credentials in CI/CD secrets:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`

Never commit credentials to version control.

## Architecture Context

### Why Separate Python Script?

Terraform Snowflake provider limitations (as of January 2025):
- **Supported**: External Volume (added in v0.90.0)
- **Not Supported**: Catalog Integration, Iceberg Table (issue #2249, Dec 2023)

Industry standard pattern:
- Terraform: Long-lived infrastructure (IAM, databases, warehouses)
- Python/CLI: Advanced vendor-specific features not yet in Terraform
- dbt: Data transformations

See docs/architecture/automation_research.md for detailed research.

### Migration Path

When Terraform adds support:
1. Export Python-created resources to Terraform state
2. Remove Python script
3. Manage all resources via Terraform

Until then, this script follows HashiCorp's guidance to avoid Terraform provisioners.

## Data Flow Integration

Delta Direct fits into the medallion architecture:

```
Batch CSV Files     --> S3 Bronze --> Spark ETL --> Delta Lake Silver --> Iceberg Table --> Snowflake Gold
Kafka Events        --> S3 Bronze --> Spark ETL --> Delta Lake Silver --> Iceberg Table --> Snowflake Gold
                                                         (S3)             (AUTO_REFRESH)      (dbt models)
```

This script creates the Iceberg Table bridge between Delta Lake Silver and Snowflake Gold layers.

## Next Steps After Deployment

1. **Verify Data Access**
   ```sql
   SELECT COUNT(*) FROM SALES_DW.RAW.SALES_SILVER_EXTERNAL;
   ```

2. **Update dbt Sources**
   - Edit `dbt/models/staging/sources.yml`
   - Point source table to `SALES_SILVER_EXTERNAL` instead of `SALES_COMBINED_STAGING`

3. **Run dbt Transformations**
   ```bash
   cd dbt
   dbt run --target prod
   ```

4. **Update Analytics DAG**
   - Remove merge_snowflake_data task
   - Update dependencies to use Delta Direct table

## Support

For issues with:
- **Terraform deployment**: See infrastructure/terraform/README.md
- **Delta Direct concepts**: See explanation_deploy.md (root directory)
- **Research findings**: See docs/architecture/automation_research.md
- **Snowflake errors**: Check Snowflake query history in web UI

## Version History

- **v1.0** (2025-01-04): Initial release
  - Catalog Integration creation
  - Iceberg Table creation with AUTO_REFRESH
  - Permission grants
  - Create/destroy modes
  - Idempotent operations
