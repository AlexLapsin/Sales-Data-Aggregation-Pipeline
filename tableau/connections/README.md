# Tableau - Snowflake Connection Setup

This guide explains how to connect Tableau Desktop to the Sales Analytics Snowflake data warehouse.

## Prerequisites

- Tableau Desktop (2021.1 or later recommended)
- Access to Snowflake account (SALES_DW database)
- Snowflake credentials (key-pair or username/password)

## Connection Methods

### Method 1: Key-Pair Authentication (Recommended)

**Advantages:**
- No MFA prompts
- More secure than passwords
- Automated refresh capabilities

**Setup Steps:**

1. **Open Tableau Desktop**
   - Click "Connect" → "To a Server" → "Snowflake"

2. **Enter Connection Details:**
   ```
   Server: <SNOWFLAKE_ACCOUNT>.snowflakecomputing.com
   ```
   Example: `UJMMNPX-ZEC04433.snowflakecomputing.com`

3. **Authentication:**
   - Select "Sign in using: Key Pair"
   - Username: Your Snowflake username (e.g., `SIRIUSLABS`)
   - Private Key File: Browse to your `.p8` key file
   - Passphrase: Enter your key passphrase

4. **Warehouse and Schema:**
   ```
   Warehouse: COMPUTE_WH (or your warehouse name)
   Database: SALES_DW
   Schema: MARTS
   Role: ANALYTICS_ROLE
   ```

5. **Click "Sign In"**

### Method 2: Username/Password Authentication

**Note:** Requires MFA setup if enabled on your account.

1. **Open Tableau Desktop**
   - Connect → Snowflake

2. **Enter Details:**
   ```
   Server: <SNOWFLAKE_ACCOUNT>.snowflakecomputing.com
   Username: Your Snowflake username
   Password: Your Snowflake password
   Warehouse: COMPUTE_WH
   Database: SALES_DW
   Schema: MARTS
   Role: ANALYTICS_ROLE
   ```

3. **Complete MFA if prompted**

## Initial SQL (Optional but Recommended)

After connecting, go to Data → Initial SQL and enter:

```sql
USE WAREHOUSE COMPUTE_WH;
USE ROLE ANALYTICS_ROLE;
USE DATABASE SALES_DW;
USE SCHEMA MARTS;
```

This ensures proper context for all queries.

## Verifying the Connection

After connecting, you should see these tables in the left panel:

- **FACT_SALES** - Main sales transactions fact table
- **DIM_DATE** - Date dimension with calendar attributes
- **DIM_CUSTOMER** - Customer dimension with segmentation
- **DIM_PRODUCT** - Product dimension with categories

If you don't see these tables:
1. Verify you selected `SALES_DW.MARTS` schema
2. Check your `ANALYTICS_ROLE` has SELECT permissions
3. Ensure the analytics_pipeline DAG has run successfully

## Performance Tips

### Use Live Connection
For this dataset (50k records), use a **live connection** instead of extract:
- Real-time data access
- Leverages Snowflake's compute power
- No extract refresh needed

### Query Optimization
- Use Tableau's relationship model (don't use joins in custom SQL)
- Let Tableau generate efficient queries
- Snowflake result cache will speed up repeated queries

### Warehouse Sizing
- **SMALL warehouse** is sufficient for this dataset
- Queries typically complete in <1 second
- Auto-suspend after 5 minutes of inactivity

## Troubleshooting

### "Unable to connect to Snowflake"
- Verify server URL format: `ACCOUNT.snowflakecomputing.com`
- Check network/firewall (Snowflake uses port 443)
- Confirm credentials are correct

### "Database/Schema not found"
- Run Initial SQL to set context
- Verify ANALYTICS_ROLE has access to SALES_DW.MARTS

### "Key pair authentication failed"
- Verify `.p8` file path is correct
- Check passphrase matches the key
- Ensure public key is registered in Snowflake user profile

### MFA Issues
- Use Duo Mobile app for MFA
- Consider key-pair auth to avoid MFA prompts

## Environment Variables

The `snowflake_connection_template.json` file uses these environment variables:

```bash
SNOWFLAKE_ACCOUNT=UJMMNPX-ZEC04433
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_USER=SIRIUSLABS
SNOWFLAKE_PRIVATE_KEY_PATH=/config/keys/rsa_key.p8
SNOWFLAKE_KEY_PASSPHRASE=your_passphrase
```

## Next Steps

After connecting successfully:
1. Use the data source in `/tableau/datasources/sales_analytics.tds`
2. Open sample dashboards in `/tableau/workbooks/`
3. Read the dashboard guide: `/docs/tableau/dashboard_guide.md`

## Security Best Practices

1. **Never commit credentials** to Git
2. Use key-pair authentication for production
3. Limit ANALYTICS_ROLE to read-only SELECT permissions
4. Set up Row-Level Security (RLS) if needed for multi-tenant access
5. Use Tableau Server/Cloud for centralized credential management

## Additional Resources

- [Tableau-Snowflake Documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_snowflake.htm)
- [Snowflake Key-Pair Auth Guide](https://docs.snowflake.com/en/user-guide/key-pair-auth.html)
- Project Documentation: `/docs/tableau/`
