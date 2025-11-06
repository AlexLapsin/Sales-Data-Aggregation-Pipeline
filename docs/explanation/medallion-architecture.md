# Medallion Architecture

Understanding the Bronze, Silver, Gold layer design and its benefits.

## What is Medallion Architecture

Medallion architecture is a data design pattern that organizes data into three progressive layers of increasing quality and refinement:

**Bronze (Raw):** Immutable source data
**Silver (Refined):** Cleaned and validated data
**Gold (Analytics):** Business-ready aggregated data

This pattern emerged from Databricks best practices and has become an industry standard for data lake architecture.

## Why Three Layers

### Historical Context

Traditional data warehouses used a two-tier approach:
- Staging: Temporary landing zone
- Data Warehouse: Final dimensional model

Problems with two-tier:
- No audit trail of raw data
- Cannot replay transformations
- Schema changes break pipeline
- Difficult to debug data quality issues

Medallion architecture adds explicit layers to solve these problems.

### Layer Purposes

**Bronze solves:**
- **Data loss prevention**: Never delete source data
- **Auditability**: Track all data received
- **Reprocessing**: Replay transformations without re-extracting
- **Debugging**: Compare transformed data to original source

**Silver solves:**
- **Consistency**: Unified schema across sources
- **Quality**: Validated and cleaned data
- **Performance**: Optimized format for processing
- **Flexibility**: Supports multiple downstream consumers

**Gold solves:**
- **Business alignment**: Data organized by business domain
- **Performance**: Pre-aggregated for analytics
- **Simplicity**: Easy for BI tools and analysts
- **Governance**: Controlled access to trusted data

## Bronze Layer Design

### Purpose

Preserve raw data exactly as received from source systems.

### Characteristics

**Immutability:**
- Never modify or delete Bronze files
- Every file is a historical record
- Enables time travel and audit

**Schema-on-Read:**
- Store data in original format (CSV, JSON)
- No schema enforcement at write time
- Flexibility for schema evolution

**Partitioning:**
```
sales_data/
├── year=2024/
│   ├── month=01/
│   │   ├── day=15/
│   │   │   └── sales_20240115.csv
```

Benefits:
- Efficient queries by date range
- Easy data lifecycle management
- Supports incremental processing

**Cost Optimization:**
- Lifecycle policies (delete after 90 days)
- Compression (gzip for CSV, native for JSON)
- S3 Intelligent-Tiering for older data

### When to Use Bronze

**Read from Bronze when:**
- Reprocessing data after bug fixes
- Investigating data quality issues
- Comparing transformed data to source
- Regulatory compliance audits

**Do not read from Bronze for:**
- Production analytics queries
- Real-time dashboards
- Ad-hoc analysis

## Silver Layer Design

### Purpose

Create a reliable, validated foundation for all analytics.

### Characteristics

**Delta Lake Format:**
- ACID transactions for data integrity
- Schema enforcement and evolution
- Time travel for historical queries
- Efficient upserts and deletes

**Data Quality:**
- Schema validation (24-field canonical)
- Null handling and defaults
- Deduplication (by order_id)
- Range checks (sales >= 0, etc.)

**Unified Schema:**

All data sources (batch CSV, streaming JSON) converge to single schema:

```
row_id, order_id, order_date, ship_date, ship_mode,
customer_id, customer_name, segment, city, state,
country, postal_code, market, region, product_id,
category, sub_category, product_name, sales,
quantity, discount, profit, shipping_cost, order_priority
```

This enables:
- Joining batch and streaming data
- Consistent downstream transformations
- Single source of truth

**Transaction Log:**

```
_delta_log/
├── 00000000000000000000.json  # Commit 0
├── 00000000000000000001.json  # Commit 1
└── 00000000000000000002.json  # Commit 2
```

Each commit records:
- Files added/removed
- Schema changes
- Operation metadata
- Timestamp

**Optimizations:**
- Z-ordering for common filters
- Auto-compaction for small files
- Partition pruning

### When to Use Silver

**Read from Silver for:**
- Feature engineering (ML pipelines)
- Complex transformations
- Cross-functional analytics
- Data science exploration

**Write to Silver for:**
- All batch processing
- All streaming processing
- Data quality corrections
- Schema evolution

## Gold Layer Design

### Purpose

Provide business-ready data optimized for analytics and reporting.

### Characteristics

**Dimensional Model:**

Star schema with fact and dimension tables:

```
fact_sales (center)
├── dim_date (FK: order_date_key)
├── dim_customer (FK: customer_key)
└── dim_product (FK: product_key)
```

Benefits:
- Simplified joins for BI tools
- Intuitive for business users
- Optimized query performance

**Pre-Aggregation:**

Fact table contains calculated measures:
- sales_amount
- profit
- quantity

Dimensions contain attributes:
- Customer segment, location
- Product category, subcategory
- Date attributes (quarter, month, week)

**Snowflake Optimizations:**

Clustering keys:
```sql
CLUSTER BY (order_date_key, customer_key)
```

Improves query performance for:
- Date range queries (90% of analytics)
- Customer cohort analysis

Materialized views for common aggregations:
```sql
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT
    order_date_key,
    SUM(sales_amount) as total_sales
FROM fact_sales
GROUP BY order_date_key;
```

**Incremental Updates:**

dbt incremental models:
```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ source('silver', 'sales') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

Only processes new data, not full table refresh.

### When to Use Gold

**Read from Gold for:**
- Business intelligence dashboards (Tableau, Power BI)
- Executive reports
- Self-service analytics
- Application databases

**Do not use Gold for:**
- Ad-hoc data science (use Silver)
- Raw data investigations (use Bronze)
- Realtime streaming analytics (use Silver)

## Data Flow Through Layers

### Batch Processing

```
CSV File Upload
    ↓
Bronze S3 (audit trail)
    ↓ [Spark ETL: validate, clean, dedupe]
Silver Delta Lake (reliable foundation)
    ↓ [dbt: transform, aggregate, dimension]
Gold Snowflake (business-ready)
```

Time: 5-8 minutes end-to-end

### Streaming Processing

```
Kafka Event
    ↓
Bronze S3 (event log)
    ↓ [Spark Streaming: validate, merge]
Silver Delta Lake (unified with batch)
    ↓ [dbt: incremental refresh]
Gold Snowflake (near-realtime)
```

Time: 3-5 minutes latency

## Benefits Realized

### Data Quality

**Bronze:**
- Zero data loss
- Complete audit trail
- Source system debugging

**Silver:**
- Schema validation
- Duplicate removal
- Null handling
- Type enforcement

**Gold:**
- Referential integrity
- Business rule validation
- Aggregation accuracy

### Performance

**Bronze:**
- Partitioned reads (scan only needed dates)
- Compression reduces storage 70%

**Silver:**
- Columnar format (Parquet) 10x faster queries
- Delta Lake indexing
- Incremental processing

**Gold:**
- Clustering keys 5x faster date queries
- Pre-aggregated measures
- Optimized for BI tool query patterns

### Cost

**Bronze:**
- S3 storage: $0.023/GB/month
- Lifecycle to Glacier after 90 days: $0.004/GB/month
- Estimated: $10-20/month for 1TB

**Silver:**
- Delta Lake compression saves 60% vs Bronze
- Estimated: $15-30/month for 1TB

**Gold:**
- Snowflake storage: $23/TB/month
- Compute: Only when queries run
- Auto-suspend saves 80% compute cost
- Estimated: $30-50/month for analytics workload

Total: $55-100/month for complete pipeline

### Flexibility

**Schema Evolution:**

Add field to source:
1. Bronze: No changes (schema-on-read)
2. Silver: ALTER TABLE ADD COLUMN
3. Gold: dbt model update

Backfill automatically from Bronze.

**Multiple Consumers:**

Silver serves:
- Gold dimensional model (via dbt)
- ML feature store (via Databricks)
- Data science notebooks (direct Delta reads)
- Streaming applications (Delta streaming)

## Trade-offs

### Complexity

**Cost:** More layers = more code + more infrastructure

**Benefit:** Clear separation of concerns, easier debugging

**Mitigation:** Automation via Airflow, standardized patterns

### Storage

**Cost:** Data replicated across layers

**Benefit:** Performance, quality, flexibility

**Mitigation:** Lifecycle policies, compression, selective retention

### Latency

**Cost:** Additional processing steps

**Benefit:** Data quality and reliability

**Mitigation:** Incremental processing, parallelization, streaming

## Alternatives Considered

### Two-Tier (Staging + Warehouse)

**Pros:**
- Simpler architecture
- Fewer processing steps

**Cons:**
- No audit trail
- Cannot reprocess
- Tight coupling to warehouse schema

**Decision:** Rejected due to lack of audit trail and flexibility

### Four-Tier (Bronze + Silver + Gold + Platinum)

**Pros:**
- More granular layers
- Platinum for ML features

**Cons:**
- Over-engineering for this use case
- Unnecessary complexity

**Decision:** Rejected, three layers sufficient

### Direct Load (Source → Warehouse)

**Pros:**
- Lowest latency
- Simplest architecture

**Cons:**
- No data lake benefits
- Expensive warehouse storage for raw data
- No ability to reprocess

**Decision:** Rejected, not suitable for modern analytics

## Best Practices

**Bronze:**
- Never delete, only add
- Compress all files
- Partition by date
- Document source system metadata

**Silver:**
- Enforce schema strictly
- Run data quality tests
- Use Delta Lake ACID
- Version all transformations

**Gold:**
- Design for query patterns
- Use clustering keys
- Implement SCD where needed
- Document business logic

## Monitoring

**Bronze Health:**
- File arrival rate
- Schema drift detection
- Partition completeness

**Silver Health:**
- Delta Lake version growth
- Data quality test pass rate
- Record count growth vs Bronze

**Gold Health:**
- Query performance metrics
- Table freshness
- Referential integrity checks

## Further Reading

- Databricks Medallion Architecture Guide
- Delta Lake Best Practices
- Kimball Dimensional Modeling
- Data Vault 2.0 (alternative approach)
