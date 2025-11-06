# Data Dictionary

Schema definitions and field descriptions for all data layers.

## Canonical Schema (24 Fields)

All layers maintain this unified schema for consistency:

| Field | Type | Description | Example | Nullable |
|-------|------|-------------|---------|----------|
| row_id | INTEGER | Unique row identifier | 1 | No |
| order_id | STRING | Unique order identifier | ORD-2024-001 | No |
| order_date | DATE | Order placement date | 2024-01-15 | No |
| ship_date | DATE | Order shipment date | 2024-01-17 | Yes |
| ship_mode | STRING | Shipping method | Standard Class | No |
| customer_id | STRING | Unique customer identifier | CUST-12345 | No |
| customer_name | STRING | Customer full name | John Smith | No |
| segment | STRING | Customer segment | Consumer | No |
| city | STRING | Customer city | New York | No |
| state | STRING | Customer state/province | NY | No |
| country | STRING | Customer country | United States | No |
| postal_code | STRING | Postal/ZIP code | 10001 | Yes |
| market | STRING | Geographic market | US | No |
| region | STRING | Sales region | East | No |
| product_id | STRING | Unique product identifier | PROD-001 | No |
| category | STRING | Product category | Office Supplies | No |
| sub_category | STRING | Product sub-category | Binders | No |
| product_name | STRING | Product full name | Wilson Jones Binder | No |
| sales | DECIMAL(10,2) | Sales amount in USD | 245.67 | No |
| quantity | INTEGER | Quantity ordered | 3 | No |
| discount | DECIMAL(3,2) | Discount percentage | 0.10 | No |
| profit | DECIMAL(10,2) | Profit amount in USD | 45.30 | No |
| shipping_cost | DECIMAL(10,2) | Shipping cost in USD | 12.50 | No |
| order_priority | STRING | Order priority level | Medium | No |

## Bronze Layer

### Batch Files (CSV)

**Location:** `s3://YOUR_RAW_BUCKET/sales_data/year=YYYY/month=MM/day=DD/`

**Format:** CSV with header row

**Partitioning:** Date-based (year/month/day)

**Schema:** 24 fields matching canonical schema

**Sample:**
```csv
Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,...
1,ORD-2024-001,2024-01-15,2024-01-17,Standard Class,CUST-12345,John Smith,...
```

**Characteristics:**
- Raw data as received from source systems
- No data quality validation applied
- May contain duplicates
- May contain null values in optional fields
- Immutable audit trail

### Streaming Files (JSON)

**Location:** `s3://YOUR_RAW_BUCKET/sales_data/sales_events/year=YYYY/month=MM/day=DD/hour=HH/`

**Format:** JSON (one record per line)

**Partitioning:** Date and hour-based

**Schema:** 24 fields matching canonical schema

**Sample:**
```json
{"row_id": 1, "order_id": "ORD-2024-001", "order_date": "2024-01-15", "ship_date": "2024-01-17", "ship_mode": "Standard Class", "customer_id": "CUST-12345", "customer_name": "John Smith", "segment": "Consumer", "city": "New York", "state": "NY", "country": "United States", "postal_code": "10001", "market": "US", "region": "East", "product_id": "PROD-001", "category": "Office Supplies", "sub_category": "Binders", "product_name": "Wilson Jones Binder", "sales": 245.67, "quantity": 3, "discount": 0.10, "profit": 45.30, "shipping_cost": 12.50, "order_priority": "Medium"}
```

**Characteristics:**
- Real-time event stream from Kafka
- Written by Kafka Connect S3 Sink
- Micro-batches (1000 records or 60 seconds)
- Same schema as batch for unified processing

## Silver Layer

### Delta Lake Tables

**Location:** `s3://YOUR_PROCESSED_BUCKET/silver/sales/`

**Format:** Delta Lake (Parquet + transaction log)

**Schema:** 24 fields + metadata columns

| Field | Type | Description |
|-------|------|-------------|
| All 24 canonical fields | Various | Same as Bronze |
| _ingestion_timestamp | TIMESTAMP | When record was ingested to Silver |
| _source_system | STRING | Source system (batch or streaming) |
| _delta_version | LONG | Delta Lake version number |

**Partitioning:** None (Delta Lake manages files automatically)

**Constraints:**
- Primary key: order_id (enforced via deduplication)
- Not null: order_id, order_date, customer_id, product_id
- Check: sales >= 0, quantity > 0, discount >= 0 AND discount <= 1

**Data Quality:**
- Deduplicated by order_id (latest wins)
- Null values replaced with defaults or filtered
- Date formats standardized to ISO 8601
- Numeric fields validated for range

**Delta Lake Features:**
- ACID transactions
- Time travel (30 days of history)
- Schema evolution
- Optimized writes

**Sample Query:**
```sql
SELECT * FROM delta.`s3://YOUR_PROCESSED_BUCKET/silver/sales/`
WHERE order_date >= '2024-01-01'
LIMIT 10;
```

## Gold Layer

### Star Schema Design

**Database:** SALES_DB
**Schema:** PUBLIC

### Fact Table: fact_sales

**Purpose:** Sales transactions with measures

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| sales_key | NUMBER | Surrogate key | Generated |
| order_id | VARCHAR | Business key | Silver.order_id |
| order_date_key | NUMBER | Date dimension FK | Join to dim_date |
| ship_date_key | NUMBER | Date dimension FK | Join to dim_date |
| customer_key | NUMBER | Customer dimension FK | Join to dim_customer |
| product_key | NUMBER | Product dimension FK | Join to dim_product |
| sales_amount | NUMBER(10,2) | Sales in USD | Silver.sales |
| quantity | NUMBER | Quantity sold | Silver.quantity |
| discount | NUMBER(3,2) | Discount percentage | Silver.discount |
| profit | NUMBER(10,2) | Profit in USD | Silver.profit |
| shipping_cost | NUMBER(10,2) | Shipping cost | Silver.shipping_cost |
| ship_mode | VARCHAR | Shipping method | Silver.ship_mode |
| order_priority | VARCHAR | Order priority | Silver.order_priority |
| created_at | TIMESTAMP_NTZ | Record creation time | Current timestamp |
| updated_at | TIMESTAMP_NTZ | Record update time | Current timestamp |

**Primary Key:** sales_key

**Foreign Keys:**
- order_date_key → dim_date.date_key
- ship_date_key → dim_date.date_key
- customer_key → dim_customer.customer_key
- product_key → dim_product.product_key

**Clustering Keys:** order_date_key, customer_key

**Measures:**
- sales_amount: Additive
- quantity: Additive
- profit: Additive
- shipping_cost: Additive
- discount: Non-additive (average for aggregations)

### Dimension: dim_customer

**Purpose:** Customer attributes and segmentation

| Column | Type | Description | SCD Type |
|--------|------|-------------|----------|
| customer_key | NUMBER | Surrogate key | - |
| customer_id | VARCHAR | Business key | - |
| customer_name | VARCHAR | Full name | 1 |
| segment | VARCHAR | Customer segment | 1 |
| city | VARCHAR | City | 1 |
| state | VARCHAR | State/province | 1 |
| country | VARCHAR | Country | 1 |
| postal_code | VARCHAR | Postal code | 1 |
| market | VARCHAR | Geographic market | 1 |
| region | VARCHAR | Sales region | 1 |
| created_at | TIMESTAMP_NTZ | Record creation | - |
| updated_at | TIMESTAMP_NTZ | Record update | - |

**Primary Key:** customer_key

**Natural Key:** customer_id

**SCD Type:** Type 1 (overwrite changes)

**Sample Values:**
- segment: Consumer, Corporate, Home Office
- market: US, APAC, EU, LATAM, Africa
- region: East, West, Central, South

### Dimension: dim_product

**Purpose:** Product hierarchy and categorization

| Column | Type | Description | SCD Type |
|--------|------|-------------|----------|
| product_key | NUMBER | Surrogate key | - |
| product_id | VARCHAR | Business key | - |
| product_name | VARCHAR | Product name | 1 |
| category | VARCHAR | Product category | 1 |
| sub_category | VARCHAR | Product sub-category | 1 |
| created_at | TIMESTAMP_NTZ | Record creation | - |
| updated_at | TIMESTAMP_NTZ | Record update | - |

**Primary Key:** product_key

**Natural Key:** product_id

**SCD Type:** Type 1 (overwrite changes)

**Category Values:**
- Office Supplies
- Technology
- Furniture

**Sub-Category Examples:**
- Office Supplies: Binders, Paper, Pens, Storage
- Technology: Phones, Accessories, Machines, Copiers
- Furniture: Chairs, Tables, Bookcases, Furnishings

### Dimension: dim_date

**Purpose:** Date attributes for time-based analysis

| Column | Type | Description |
|--------|------|-------------|
| date_key | NUMBER | Surrogate key (YYYYMMDD) |
| date | DATE | Actual date |
| day_of_week | NUMBER | 1-7 (Monday-Sunday) |
| day_name | VARCHAR | Monday, Tuesday, etc. |
| day_of_month | NUMBER | 1-31 |
| day_of_year | NUMBER | 1-366 |
| week_of_year | NUMBER | 1-53 |
| month | NUMBER | 1-12 |
| month_name | VARCHAR | January, February, etc. |
| quarter | NUMBER | 1-4 |
| year | NUMBER | YYYY |
| is_weekend | BOOLEAN | True for Sat/Sun |
| is_holiday | BOOLEAN | True for holidays |
| fiscal_year | NUMBER | Fiscal year |
| fiscal_quarter | NUMBER | Fiscal quarter |

**Primary Key:** date_key

**Range:** 2010-01-01 to 2030-12-31

**Population:** Pre-populated via dbt macro

## Data Types

### Type Mappings Across Layers

| Canonical Type | Bronze CSV | Silver Delta | Gold Snowflake |
|----------------|------------|--------------|----------------|
| INTEGER | String | INT | NUMBER |
| DECIMAL(10,2) | String | DECIMAL(10,2) | NUMBER(10,2) |
| STRING | String | STRING | VARCHAR |
| DATE | String (YYYY-MM-DD) | DATE | DATE |
| TIMESTAMP | String (ISO 8601) | TIMESTAMP | TIMESTAMP_NTZ |

### Special Handling

**Dates:**
- Bronze: String in various formats
- Silver: Standardized to ISO 8601 (YYYY-MM-DD)
- Gold: DATE type with dimension lookup

**Nulls:**
- Bronze: Empty strings or NULL
- Silver: NULL (validated)
- Gold: NULL or default values per business rules

**Numeric Precision:**
- Sales/Profit: 2 decimal places
- Discount: 2 decimal places (0.00-1.00)
- Quantity: Integer, no decimals

## Data Quality Rules

### Silver Layer Validation

**Required Fields:**
- order_id, order_date, customer_id, product_id must not be null

**Range Checks:**
- sales >= 0
- quantity > 0
- discount >= 0 AND discount <= 1
- profit can be negative (loss)
- shipping_cost >= 0

**Format Validation:**
- Dates in ISO 8601 format
- order_id matches pattern: ORD-YYYY-######
- customer_id matches pattern: CUST-#####
- product_id matches pattern: PROD-###

**Referential Integrity:**
- Each order must have valid customer_id
- Each order must have valid product_id

### Gold Layer Validation

**dbt Tests Applied:**

**Unique Tests:**
- fact_sales.sales_key
- dim_customer.customer_key
- dim_product.product_key
- dim_date.date_key

**Not Null Tests:**
- All surrogate keys
- All foreign keys in fact_sales
- All business keys in dimensions

**Relationship Tests:**
- fact_sales.customer_key → dim_customer.customer_key
- fact_sales.product_key → dim_product.product_key
- fact_sales.order_date_key → dim_date.date_key

**Accepted Values:**
- segment: Consumer, Corporate, Home Office
- ship_mode: Standard Class, Second Class, First Class, Same Day
- order_priority: Low, Medium, High, Critical

## Data Lineage

```
Source CSV Files
    ↓ (data_uploader.py)
Bronze S3 (CSV)
    ↓ (Spark batch_etl.py)
Silver Delta Lake
    ↓ (dbt stg_sales_silver.sql)
Staging Views
    ↓ (dbt int_sales_unified.sql)
Intermediate Tables
    ↓ (dbt fact_sales.sql, dim_*.sql)
Gold Snowflake Tables
```

## Sample Queries

### Silver Layer

```python
# PySpark query
df = spark.read.format("delta").load("s3://YOUR_PROCESSED_BUCKET/silver/sales/")
df.filter(df.order_date >= "2024-01-01").groupBy("category").sum("sales").show()
```

### Gold Layer

```sql
-- Sales by category
SELECT
    p.category,
    SUM(f.sales_amount) as total_sales,
    SUM(f.profit) as total_profit,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;

-- Monthly sales trend
SELECT
    d.year,
    d.month_name,
    SUM(f.sales_amount) as total_sales
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```
