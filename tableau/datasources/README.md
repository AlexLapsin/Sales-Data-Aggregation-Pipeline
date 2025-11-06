# Tableau Data Sources

This directory contains pre-configured Tableau data sources for the Sales Analytics pipeline.

## sales_analytics.tds

**Purpose:** Centralized, governed data source connecting to Snowflake SALES_DW.MARTS schema.

**What's Included:**

### Star Schema Relationships
```
FACT_SALES (center)
├── DIM_DATE (date_key)
├── DIM_CUSTOMER (customer_key)
└── DIM_PRODUCT (product_key)
```

### Pre-Configured Measures
| Field Name | Description | Formula |
|------------|-------------|---------|
| Sales Amount | Net sales revenue | `net_sales_amount` |
| Gross Sales | Sales before discounts | `gross_sales_amount` |
| Discount Amount | Total discounts applied | `discount_amount` |
| Profit | Profit (can be negative) | `profit_amount` |
| Quantity | Number of units sold | `quantity` |
| Shipping Cost | Shipping fees | `shipping_cost` |
| **Profit Margin %** | Profitability ratio | `profit / sales` |
| **Average Order Value** | Revenue per order | `sales / order_count` |
| **Discount Rate %** | Avg discount percentage | `discount / gross_sales` |
| **Units per Order** | Avg items per order | `sum(qty) / count(orders)` |
| **Total Orders** | Distinct order count | `COUNTD(sales_key)` |

### How to Use

#### Method 1: Open in Tableau Desktop
1. Double-click `sales_analytics.tds`
2. Tableau will prompt for Snowflake credentials
3. Enter your connection details (see `/tableau/connections/README.md`)
4. Data source opens with all relationships pre-configured

#### Method 2: Import into Existing Workbook
1. Open your Tableau workbook
2. Data → New Data Source → More... → Tableau Data Source
3. Browse to `sales_analytics.tds`
4. Click "Open"

#### Method 3: Publish to Tableau Server/Cloud
```bash
# Using Tableau Server REST API or Tableau CLI
tabcmd publish "sales_analytics.tds" \
  --name "Sales Analytics" \
  --project "Sales Analytics Pipeline" \
  --oauth-username your.email@company.com
```

### Data Source Features

**Connection Mode:** Live (recommended)
- Data size: ~50k records
- Query performance: <1 second typical
- Real-time data access
- Leverages Snowflake result cache

**Relationships:** Star Schema
- Automatically handles many-to-one joins
- No need for complex custom SQL
- Optimal query performance

**Data Refresh:** Real-time
- No extract refresh needed
- Always shows latest data from Snowflake
- Updates as soon as dbt pipeline completes

### Customizing the Data Source

To add custom calculated fields:

1. Open `sales_analytics.tds` in Tableau
2. Right-click in Data pane → Create Calculated Field
3. Examples:

```tableau
// Year-over-Year Sales Growth
(SUM([net_sales_amount]) -
 LOOKUP(SUM([net_sales_amount]), -12)) /
 LOOKUP(SUM([net_sales_amount]), -12)

// Customer Lifetime Value
WINDOW_SUM(SUM([profit_amount]))

// Running Total
RUNNING_SUM(SUM([net_sales_amount]))

// Rank by Sales
RANK(SUM([net_sales_amount]))
```

4. Save the .tds file

### Date Hierarchies

The DIM_DATE table provides these hierarchies:
- **Year** → Quarter → Month → Week → Day
- **Fiscal Year** (if defined in dbt)
- **Day of Week** (Monday, Tuesday, etc.)
- **Is Weekend** (Yes/No)

To use:
1. Drag `Date Value` from DIM_DATE to Rows/Columns
2. Click the + icon to drill down

### Product Hierarchies

Product categorization:
- **Category** → Sub-Category → Product Name
- Example: FURNITURE → Bookcases → Specific product

### Customer Segmentation

From DIM_CUSTOMER:
- **Segment**: Consumer, Corporate, Home Office
- **Value Tier**: VIP, High Value, Regular, Occasional
- **Geography**: Country → State → City

## Data Dictionary

### FACT_SALES Fields
| Field | Type | Description |
|-------|------|-------------|
| sales_key | Integer | Unique transaction ID |
| date_key | Integer | Foreign key to DIM_DATE |
| customer_key | Integer | Foreign key to DIM_CUSTOMER |
| product_key | Integer | Foreign key to DIM_PRODUCT |
| net_sales_amount | Decimal | Sales after discounts |
| gross_sales_amount | Decimal | Sales before discounts |
| discount_amount | Decimal | Total discount applied |
| profit_amount | Decimal | Profit (can be negative) |
| quantity | Integer | Units sold |
| unit_price | Decimal | Price per unit |
| shipping_cost | Decimal | Shipping fees |

### DIM_DATE Fields
| Field | Type | Description |
|-------|------|-------------|
| date_key | Integer | Surrogate key |
| date_value | Date | Actual date |
| year_number | Integer | Year (2011-2014) |
| quarter | Integer | Quarter (1-4) |
| month_number | Integer | Month (1-12) |
| month_name | String | Month name |
| day_of_week | Integer | Day (0=Sunday) |
| day_name | String | Day name |
| is_weekend | Boolean | Weekend flag |
| week_of_year | Integer | Week number |

### DIM_CUSTOMER Fields
| Field | Type | Description |
|-------|------|-------------|
| customer_key | Integer | Surrogate key |
| customer_id | String | Business key |
| customer_name | String | Full name |
| segment | String | Consumer/Corporate/Home Office |
| customer_value_tier | String | VIP/High/Regular/Occasional |
| total_spent | Decimal | Lifetime value |
| city | String | City |
| state | String | State |
| country | String | Country |
| region | String | Geographic region |

### DIM_PRODUCT Fields
| Field | Type | Description |
|-------|------|-------------|
| product_key | Integer | Surrogate key |
| product_id | String | Business key |
| product_name | String | Product name |
| category | String | FURNITURE/OFFICE SUPPLIES/TECHNOLOGY |
| sub_category | String | Product sub-category |
| is_active | Boolean | Currently selling |

## Performance Tips

1. **Use Aggregations:** Pre-aggregate where possible
2. **Limit Date Range:** Use relative date filters
3. **Index Usage:** Snowflake uses clustering keys automatically
4. **Context Filters:** Use for improved query performance
5. **Extract if Needed:** For 100k+ records or slow connections

## Troubleshooting

**"Unable to refresh data source"**
- Check Snowflake connection credentials
- Verify ANALYTICS_ROLE has SELECT on MARTS schema
- Ensure analytics_pipeline has run successfully

**"Relationship not working"**
- Verify surrogate keys match (date_key, customer_key, product_key)
- Check cardinality (many FACT_SALES to one DIM_*)
- Use Edit Relationships to verify join conditions

**"Calculated field error"**
- Check field names match exactly (case-sensitive)
- Verify aggregation functions (SUM, AVG, etc.) are used correctly
- Test calculations with simple data first

## Best Practices

1. **Don't Modify Source Data:** Use calculated fields instead
2. **Document Custom Fields:** Add comments to calculations
3. **Version Control:** Save .tds files with meaningful names
4. **Test Before Publishing:** Validate with sample dashboards
5. **Use Parameters:** For dynamic filtering and what-if analysis

## Next Steps

1. Review sample dashboards in `/tableau/workbooks/`
2. Read dashboard guide: `/docs/tableau/dashboard_guide.md`
3. Customize for your specific use cases
4. Publish to Tableau Server/Cloud for sharing

## Support

For issues or questions:
1. Check `/docs/tableau/` documentation
2. Review Snowflake query logs for performance issues
3. Verify dbt data quality tests are passing
4. Contact data engineering team
