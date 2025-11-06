# Tableau Analytics for Sales Data Pipeline

This directory contains Tableau resources for visualizing and analyzing the sales data from the Medallion architecture pipeline.

## Overview

The Sales Analytics Tableau implementation provides:
- **Pre-configured data source** connecting to Snowflake dimensional model
- **Sample dashboard specifications** for common business questions
- **Complete documentation** for setup and usage
- **Best practices** for performance and governance

## Quick Start

### 1. Set Up Snowflake Connection

**Prerequisites:**
- Tableau Desktop (2021.1+)
- Snowflake credentials (see `.env` file)
- Access to SALES_DW.MARTS schema

**Steps:**
```bash
# 1. Review connection template
cat tableau/connections/snowflake_connection_template.json

# 2. Follow detailed setup guide
see tableau/connections/README.md

# 3. Test connection in Tableau Desktop
Open Tableau → Connect → Snowflake
```

### 2. Load the Data Source

**Option A: Use Pre-Built Data Source**
```
1. Double-click: tableau/datasources/sales_analytics.tds
2. Enter Snowflake credentials when prompted
3. Start building visualizations
```

**Option B: Build From Scratch**
```
1. Connect to Snowflake (see step 1)
2. Select SALES_DW.MARTS schema
3. Add tables: FACT_SALES, DIM_DATE, DIM_CUSTOMER, DIM_PRODUCT
4. Define relationships (see Data Source guide)
```

### 3. Explore Sample Dashboards

Review dashboard specifications in `/docs/tableau/dashboard_blueprints.md`:
- Executive Dashboard (KPIs and trends)
- Sales Performance Analysis
- Product Analysis
- Customer Segmentation

## Directory Structure

```
tableau/
├── connections/
│   ├── snowflake_connection_template.json  # Connection config template
│   └── README.md                            # Detailed setup guide
├── datasources/
│   ├── sales_analytics.tds                  # Pre-configured data source
│   └── README.md                            # Data source documentation
├── workbooks/
│   └── README.md                            # Dashboard specifications
└── README.md                                # This file
```

## Data Architecture

### Star Schema Model

```
              ┌─────────────┐
              │  DIM_DATE   │
              │  date_key   │
              └──────┬──────┘
                     │
              ┌──────▼────────┐
  ┌───────────┤  FACT_SALES   ├───────────┐
  │           │  sales_key    │           │
  │           │  date_key     │           │
  │           │  customer_key │           │
  │           │  product_key  │           │
  │           │  sales        │           │
  │           │  profit       │           │
  │           │  quantity     │           │
  │           └───────────────┘           │
  │                                       │
┌─▼──────────────┐              ┌────────▼───────┐
│ DIM_CUSTOMER   │              │  DIM_PRODUCT   │
│ customer_key   │              │  product_key   │
│ segment        │              │  category      │
│ value_tier     │              │  sub_category  │
└────────────────┘              └────────────────┘
```

### Data Scope

- **Time Period:** 2011-2014
- **Record Count:** ~50,505 sales transactions
- **Granularity:** Individual order line items
- **Update Frequency:** Real-time (live connection)

## Key Features

### Pre-Configured Calculated Fields

The data source includes these calculated fields ready to use:

| Metric | Formula | Use Case |
|--------|---------|----------|
| **Profit Margin %** | profit / sales | Profitability analysis |
| **Average Order Value** | sales / order_count | Customer behavior |
| **Discount Rate %** | discount / gross_sales | Pricing strategy |
| **Units per Order** | sum(qty) / count(orders) | Basket analysis |
| **Total Orders** | COUNTD(sales_key) | Volume metrics |

### Hierarchies

**Date Hierarchy:**
Year → Quarter → Month → Week → Day

**Product Hierarchy:**
Category → Sub-Category → Product Name

**Geographic Hierarchy:**
Country → Region → State → City

### Filters and Parameters

**Recommended Filters:**
- Date Range (Relative: Last N days/months/years)
- Product Category (Multi-select)
- Customer Segment (Consumer, Corporate, Home Office)
- Geographic Region

**Suggested Parameters:**
- Target Profit Margin (for goal tracking)
- Top N Products (dynamic ranking)
- Date Comparison Period (YoY, MoM analysis)

## Dashboard Specifications

### 1. Executive Dashboard

**Purpose:** High-level business overview for leadership

**Key Metrics:**
- Total Sales, Total Profit, Profit Margin %
- Sales vs Target (parameter-driven)
- Year-over-Year Growth

**Visualizations:**
- KPI Cards (big numbers)
- Sales Trend Line (monthly)
- Sales by Category (bar chart)
- Geographic Heat Map

**Filters:**
- Date Range
- Product Category

### 2. Sales Performance Dashboard

**Purpose:** Detailed sales analysis for operations team

**Metrics:**
- Sales, Orders, AOV, Units Sold
- Discount Impact
- Shipping Costs

**Visualizations:**
- Sales by Category/Sub-Category (tree map)
- Monthly Trend with Forecast
- Discount vs Profit Scatter Plot
- Shipping Cost Analysis

**Filters:**
- Date Range
- Category
- Region
- Order Priority

### 3. Product Analysis Dashboard

**Purpose:** Product performance and profitability

**Metrics:**
- Sales, Profit, Profit Margin % per Product
- Units Sold
- Average Price

**Visualizations:**
- Product Profitability Matrix (Margin % vs Sales)
- Top 10 Products by Sales
- Bottom 10 Products by Profit Margin
- Category Comparison

**Features:**
- Dynamic Top N parameter
- Product search/filter
- Category drill-down

### 4. Customer Segmentation Dashboard

**Purpose:** Understanding customer behavior and value

**Metrics:**
- Customers by Segment
- Revenue by Value Tier
- Geographic Distribution

**Visualizations:**
- Segment Distribution (pie chart)
- Customer Value Tiers (stacked bar)
- Geographic Map (country/state)
- Customer Lifetime Value histogram

**Filters:**
- Segment
- Value Tier
- Geography

## Performance Considerations

### Connection Mode: Live

**Why Live Connection?**
- Dataset is small (~50k records)
- Queries execute in <1 second
- Always shows real-time data
- Leverages Snowflake's performance

**When to Use Extracts:**
- Dataset grows >100k records
- Network latency is high
- Offline analysis needed
- Schedule-based refreshes acceptable

### Query Optimization

**Best Practices:**
1. Use context filters for date ranges
2. Leverage Snowflake's result cache (same queries return instantly)
3. Avoid custom SQL (use relationships instead)
4. Use aggregations (SUM, AVG) at database level
5. Filter before visualizing (don't filter after)

**Warehouse Sizing:**
```sql
USE WAREHOUSE COMPUTE_WH;  -- SMALL is sufficient
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 300; -- 5 minutes
```

### Monitoring Performance

Check query performance in Snowflake:
```sql
SELECT query_text, execution_time, query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag = 'Tableau'
ORDER BY start_time DESC
LIMIT 100;
```

## Security and Governance

### Row-Level Security (RLS)

If needed, implement RLS in Snowflake:
```sql
-- Example: Restrict users to their region
CREATE OR REPLACE SECURE VIEW MARTS.FACT_SALES_SECURE AS
SELECT * FROM MARTS.FACT_SALES
WHERE region = CURRENT_USER();
```

### Data Certification

**Mark as Certified Data Source:**
1. Publish to Tableau Server/Cloud
2. Project → Data Sources → Select `sales_analytics`
3. Mark as "Certified"
4. Add description and owner

### Access Control

**Snowflake Permissions:**
```sql
-- Analytics users get read-only access
GRANT SELECT ON ALL TABLES IN SCHEMA MARTS TO ROLE ANALYTICS_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA MARTS TO ROLE ANALYTICS_ROLE;
```

**Tableau Server/Cloud:**
- Create "Sales Analytics" project
- Assign permissions by group
- Enable subscriptions for dashboards

## Deployment Guide

### Local Development

1. Use `.tds` file from this repository
2. Connect with personal Snowflake credentials
3. Build and test dashboards locally
4. Save as `.twbx` (packaged workbook)

### Publishing to Tableau Server/Cloud

```bash
# Install Tableau CLI
npm install -g @tableau/tableau-server-client

# Publish data source
tabcmd publish sales_analytics.tds \
  --name "Sales Analytics - Production" \
  --project "Sales Analytics Pipeline" \
  --db-user ${SNOWFLAKE_USER} \
  --db-password ${SNOWFLAKE_PASSWORD}

# Publish workbooks
tabcmd publish Executive_Dashboard.twbx \
  --name "Executive Dashboard" \
  --project "Sales Analytics Pipeline" \
  --overwrite
```

### Scheduling Refreshes

For live connections, no refresh needed. For extracts:
```
1. Tableau Server → Schedules
2. Create "Daily Extract Refresh"
3. Time: 6:00 AM (after dbt pipeline completes)
4. Assign to data sources/workbooks
```

## Troubleshooting

### Common Issues

**Connection Fails**
```
Error: Unable to connect to Snowflake

Solution:
1. Verify server URL: ACCOUNT.snowflakecomputing.com
2. Check credentials in connection dialog
3. Test with SnowSQL first: snowsql -a ACCOUNT -u USER
4. Review firewall/network settings (port 443)
```

**Slow Performance**
```
Symptoms: Queries take >5 seconds

Solutions:
1. Check Snowflake warehouse status (suspended?)
2. Use context filters to reduce data volume
3. Review query history for inefficient queries
4. Consider using aggregated tables/views
5. Upgrade warehouse size if needed
```

**Data Not Refreshing**
```
Symptoms: Seeing stale data

Solutions:
1. Check connection type (Live vs Extract)
2. For extracts: Check refresh schedule status
3. Verify analytics_pipeline DAG ran successfully
4. Clear Tableau cache: Help → Settings → Clear Cache
```

**Relationship Errors**
```
Error: Cannot establish relationship

Solutions:
1. Verify surrogate keys match (date_key, customer_key, product_key)
2. Check Data Source tab → Edit Relationships
3. Ensure tables are from same connection
4. Review cardinality (many-to-one expected)
```

## Additional Resources

### Documentation

- **Connection Setup:** `/tableau/connections/README.md`
- **Data Source Guide:** `/tableau/datasources/README.md`
- **Dashboard Blueprints:** `/docs/tableau/dashboard_blueprints.md`
- **User Guide:** `/docs/tableau/user_guide.md`

### External Links

- [Tableau-Snowflake Best Practices](https://www.tableau.com/solutions/snowflake)
- [Star Schema in Tableau](https://help.tableau.com/current/pro/desktop/en-us/relate_tables.htm)
- [Tableau Performance Checklist](https://help.tableau.com/current/pro/desktop/en-us/perf_checklist.htm)

### Training Resources

- **Tableau Desktop I:** Core visualization skills
- **Tableau Desktop II:** Advanced analytics
- **Snowflake for Analytics:** SQL and optimization

## Contributing

To add new dashboards or metrics:

1. Create feature branch
2. Build dashboard in Tableau Desktop
3. Test with production data
4. Document in `/docs/tableau/`
5. Submit PR for review

## Support

For questions or issues:
1. Check documentation in `/docs/tableau/`
2. Review troubleshooting section above
3. Contact data engineering team
4. File issue in project repository

---

**Last Updated:** 2025-10-20
**Version:** 1.0.0
**Maintainer:** Data Engineering Team
