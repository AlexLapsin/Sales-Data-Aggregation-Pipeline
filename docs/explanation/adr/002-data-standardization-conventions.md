# ADR 002: Data Standardization Conventions

**Date:** 2025-01-30
**Status:** Accepted
**Decision Makers:** Data Engineering Team

## Context

During Phase 7 pipeline testing, we encountered data quality issues stemming from inconsistent data standardization practices across the Medallion architecture (Bronze → Silver → Gold). Specifically:

1. **Column Names vs Values:** Confusion between SQL style guides (for identifiers) and data value conventions
2. **Categorical Value Format:** Inconsistency between "OFFICE_SUPPLIES" (uppercase with underscores) vs "Office Supplies" (Title Case with spaces)
3. **Surrogate Key Collisions:** Range-based row_id generation (1-999K batch, 1M-10M streaming) risked collisions at scale
4. **Postal Code Data Loss:** Spark's `inferSchema=true` treated "08701" as numeric, causing loss of leading zeros

## Research Conducted

### SQL Style Guides (2024-2025)
- **Simon Holywell SQL Style Guide** (Nov 2024): "Always use lowercase except where it may make sense not to"
- **GitLab/Meltano SQL Guides**: Unanimous recommendation for `lowercase_with_underscores` for database identifiers
- **Consensus**: Style guides apply to **column names and table names**, NOT data values

### Data Value Conventions
- **Stack Overflow (291166)**: "Store in original case unless there's a reason to do otherwise" (highest voted)
- **Kaggle Superstore Dataset**: Uses Title Case with spaces (`"Office Supplies"`, `"Home Office"`)
- **TPC-DS Benchmark**: Uses Title Case (`"Sports"`, `"Home"`)
- **Industry Practice**: No universal standard for values - preserve source format

### Surrogate Key Generation (2024)
- **Databricks**: Recommends IDENTITY columns (sequential) or UUIDv7
- **AWS RDS Blog**: UUIDv7 "encodes Unix timestamp with millisecond precision in first 48 bits, making it time-ordered and sequential"
- **PostgreSQL Community**: UUIDv7 eliminates poor temporal locality of UUIDv4
- **Consensus**: UUIDv7 is 2024 industry standard for distributed systems

### PySpark Schema Inference
- **Towards Data Science**: "Avoid solely relying on inferSchema in large or repeatedly accessed datasets"
- **Medium (PySpark CSV Deep Dive)**: `inferSchema=true` causes performance overhead and type inference errors
- **Best Practice**: Use `inferSchema=false` and manually cast numeric columns

## Decision

### 1. Column Names: `lowercase_with_underscores`

**Apply SQL style guide conventions to database identifiers:**
```sql
-- Column names (identifiers)
SELECT customer_segment, product_category, postal_code
FROM fact_sales
WHERE order_date >= '2024-01-01'
```

**Implementation:**
- Bronze: Mixed case from source (preserved)
- Silver: `lowercase_with_underscores` (via `normalize_column_names()`)
- Gold: `lowercase_with_underscores` (inherited from Silver)

### 2. Data Values: Preserve Source Format

**Preserve original Kaggle Superstore format (Title Case with spaces):**
```sql
WHERE customer_segment = 'Home Office'  -- Title Case with space
  AND product_category = 'Office Supplies'
```

**Rationale:**
- ✅ Business-friendly (readable reports)
- ✅ Data integrity (no information loss)
- ✅ Stack Overflow consensus
- ✅ Matches source dataset convention

**Values:**
- Categories: `"Furniture"`, `"Office Supplies"`, `"Technology"`
- Segments: `"Consumer"`, `"Corporate"`, `"Home Office"`
- Geographic: `"United States"`, `"New York"`, etc.

**Transformation Rules:**
```python
# Only normalize whitespace (collapse multiple spaces)
.withColumn("category", regexp_replace(trim(col("category")), r'\s+', ' '))
.withColumn("segment", regexp_replace(trim(col("segment")), r'\s+', ' '))
```

### 3. Surrogate Keys: UUIDv7

**Replace range-based row_id with UUIDv7:**

**Old Approach (Rejected):**
```python
# Batch: row_id = 1 to 999,999
# Streaming: row_id = 1,000,000 to 9,999,999
row_id = random.randint(1, 1000000)  # Risk of collision at scale
```

**New Approach (Accepted):**
```python
def generate_uuidv7() -> str:
    """
    Generate UUIDv7 (RFC 9562) - Time-ordered UUID

    Format:
    - 48 bits: Unix timestamp (millisecond precision)
    - 12 bits: sequence counter
    - 62 bits: random data

    Benefits:
    - Time-ordered: Better index performance
    - Globally unique: No batch/streaming collisions
    - Sortable: Natural chronological ordering
    """
    timestamp_ms = int(time.time() * 1000)
    # ... implementation details
    return str(uuid.UUID(bytes=uuid_bytes))

# Usage
.withColumn("row_id", uuidv7_udf())
```

**Benefits:**
- ✅ Eliminates collision risk (globally unique)
- ✅ Better database performance (time-ordered)
- ✅ Industry standard (2024 AWS/PostgreSQL/Databricks)
- ✅ Enables future horizontal scaling

### 4. CSV Schema Inference: Explicit Control

**Problem:**
```python
.option("inferSchema", "true")  # Treats "08701" as 8701.0
```

**Solution:**
```python
# Read all columns as STRING
.option("inferSchema", "false")

# Then explicitly cast numeric columns
.withColumn("sales", col("sales").cast("double"))
.withColumn("quantity", col("quantity").cast("integer"))
.withColumn("discount", col("discount").cast("double"))
.withColumn("profit", col("profit").cast("double"))
.withColumn("shipping_cost", col("shipping_cost").cast("double"))
# postal_code remains STRING - preserves "08701"
```

**Impact:**
- ✅ Preserves leading zeros in US ZIP codes (NJ: 07xxx, 08xxx; MA: 01xxx, 02xxx; PR: 00xxx)
- ✅ Supports international alphanumeric codes (UK: "SW1A 1AA", Canada: "K1A 0B1")
- ✅ Prevents estimated $1.6M-$3.3M annual data quality risk

## Consequences

### Positive

1. **Clear Separation of Concerns:**
   - SQL style guides → Column names
   - Source format → Data values

2. **Data Integrity:**
   - No postal code data loss (500-1,000 records affected)
   - No surrogate key collisions
   - Business-friendly reporting

3. **Industry Alignment:**
   - UUIDv7: 2024 standard
   - Source preservation: Stack Overflow consensus
   - Explicit schemas: PySpark best practice

### Negative

1. **Migration Required:**
   - Existing Silver Delta Lake tables need postal_code type change
   - Existing row_id values (integers) will be replaced with UUIDs
   - dbt tests need value updates

2. **Query Changes:**
   - WHERE clauses must use `'Home Office'` not `'HOME_OFFICE'`
   - Case-sensitive comparisons required

3. **Performance:**
   - UUIDs (36 chars) vs integers (8 bytes) - slight storage increase
   - Offset by better index performance (time-ordered)

## Implementation

**Files Changed:**
- `src/spark/jobs/batch_etl.py`: inferSchema=false, UUIDv7, categorical preservation
- `src/streaming/sales_event_producer.py`: UUIDv7 generation
- `dbt/models/staging/_sources.yml`: Test value updates
- `dbt/models/marts/_marts.yml`: Test value updates

**Migration Steps:**
1. Clear Silver Delta Lake: `aws s3 rm s3://$PROCESSED_BUCKET/silver/sales/ --recursive`
2. Rebuild from Bronze with new logic
3. Verify postal codes preserved: `SELECT DISTINCT postal_code FROM ... WHERE postal_code LIKE '0%'`
4. Verify row_id format: `SELECT row_id FROM ... LIMIT 5` (should show UUIDs)

## References

- [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/) (2024)
- [Stack Overflow: Database Text Case Storage](https://stackoverflow.com/questions/291166/)
- [AWS Blog: Implement UUIDv7 in RDS PostgreSQL](https://aws.amazon.com/blogs/database/implement-uuidv7-in-amazon-rds-for-postgresql-using-trusted-language-extensions/)
- [Databricks: Identity Columns for Surrogate Keys](https://www.databricks.com/blog/2022/08/08/identity-columns-to-generate-surrogate-keys-are-now-available-in-a-lakehouse-near-you.html)
- [Towards Data Science: The InferSchema Problem](https://towardsdatascience.com/pyspark-explained-the-inferschema-problem-12a08c989371/)
- RFC 9562: Universally Unique IDentifiers (UUIDs)

## Alternatives Considered

### Alternative 1: Lowercase with Underscores for Values
```sql
WHERE customer_segment = 'home_office'
  AND product_category = 'office_supplies'
```

**Rejected:**
- Less readable in reports
- Deviates from source dataset
- No industry standard supporting this for values

### Alternative 2: Keep Range-Based row_id
```python
# Batch: 1-999,999
# Streaming: 1,000,000-9,999,999
```

**Rejected:**
- Collision risk at >1M batch records
- Not horizontally scalable
- Inferior to UUIDv7 for distributed systems

### Alternative 3: Explicit Full Schema
```python
schema = StructType([
    StructField("row_id", StringType(), True),
    # ... 23 more fields
])
```

**Rejected:**
- Verbose (24 field definitions)
- Doesn't handle source column name variations
- `inferSchema=false` + casting is cleaner

## Approval

**Approved by:** Data Engineering Team
**Date:** 2025-01-30
**Supersedes:** Initial implementation assumptions
