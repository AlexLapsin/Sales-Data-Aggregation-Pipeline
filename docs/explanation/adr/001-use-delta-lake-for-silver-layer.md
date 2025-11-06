# ADR 001: Use Delta Lake for Silver Layer Storage

## Status

Accepted

## Context

The Silver layer requires a storage format that provides data quality guarantees while supporting both batch and streaming workloads. The format must handle concurrent reads/writes, schema evolution, and allow time-based queries.

### Requirements

1. ACID transaction support for data integrity
2. Schema enforcement to prevent data quality issues
3. Efficient upserts for streaming data merges
4. Time travel capability for debugging and audits
5. Compatibility with Spark processing engine
6. Support for schema evolution without rewriting data
7. Open-source to avoid vendor lock-in

### Options Considered

**Option 1: Parquet Files Only**

Pros:
- Simple, widely supported format
- Efficient columnar storage
- Good compression ratios
- Native Spark support

Cons:
- No ACID guarantees
- No schema enforcement
- Cannot update or delete records efficiently
- No time travel
- Concurrent writes cause conflicts

**Option 2: Apache Hudi**

Pros:
- ACID transactions
- Record-level updates and deletes
- Incremental processing
- Timeline for time travel

Cons:
- More complex than Delta Lake
- Steeper learning curve
- Smaller community and ecosystem
- Less integration with Databricks
- Performance overhead for small files

**Option 3: Apache Iceberg**

Pros:
- Vendor-neutral table format
- Multi-engine support (Spark, Flink, Trino)
- ACID transactions
- Schema evolution
- Time travel

Cons:
- Less mature than Delta Lake (as of 2024)
- Smaller ecosystem
- Less Snowflake integration
- Fewer optimization features

**Option 4: Delta Lake**

Pros:
- Full ACID transactions
- Schema enforcement and evolution
- Efficient merge operations (upserts)
- Time travel (30+ days of history)
- Transaction log for debugging
- Optimized writes and compaction
- Strong Databricks integration
- Direct Snowflake querying (Delta Direct)
- Large, active community

Cons:
- Originally Databricks-led (though open-source)
- Requires Delta-aware readers (not all tools support)

## Decision

Use Delta Lake for Silver layer storage.

### Rationale

1. **ACID Guarantees:**
   - Prevents data corruption from failed writes
   - Safe concurrent reads and writes
   - Critical for production reliability

2. **Schema Enforcement:**
   ```python
   # Write fails if schema mismatches
   df.write.format("delta").mode("append").save(path)
   ```
   Prevents data quality issues from upstream changes.

3. **Efficient Streaming Merges:**
   ```python
   deltaTable.alias("target").merge(
       streamingDF.alias("source"),
       "target.order_id = source.order_id"
   ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
   ```
   Enables real-time streaming deduplication.

4. **Time Travel:**
   ```python
   # Rollback to yesterday's data
   df = spark.read.format("delta") \
       .option("timestampAsOf", "2024-01-14") \
       .load("s3://bucket/silver/sales/")
   ```
   Essential for debugging and recovery.

5. **Snowflake Integration:**
   - Snowflake can query Delta tables directly via Delta Direct
   - Zero data duplication
   - Automatic schema sync

6. **Databricks Compatibility:**
   - Native support in Databricks runtime
   - Optimized performance
   - Auto-compaction and optimization

## Consequences

### Positive

- Reliable data foundation with ACID guarantees
- No data quality issues from schema drift
- Efficient streaming and batch processing
- Easy debugging with time travel
- Direct Snowflake access without ETL
- Strong ecosystem support

### Negative

- Requires Delta-aware readers (not all BI tools)
- Slight storage overhead for transaction log
- Learning curve for operations team
- Dependency on Delta Lake project (mitigated by open-source)

### Mitigation

- Use Snowflake Delta Direct for BI tool access
- Implement lifecycle policies for old transaction logs
- Provide training documentation for Delta operations
- Monitor Delta Lake project health and community

## Implementation

1. Add Delta Lake dependencies:
   ```
   delta-spark==3.0.0
   ```

2. Configure Spark sessions:
   ```python
   spark = SparkSession.builder \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
       .getOrCreate()
   ```

3. Write Delta tables:
   ```python
   df.write.format("delta").mode("append").save(silver_path)
   ```

4. Configure Snowflake Delta Direct:
   ```sql
   CREATE EXTERNAL VOLUME delta_volume ...;
   CREATE ICEBERG TABLE sales_silver ...;
   ```

## References

- Delta Lake Documentation: https://docs.delta.io
- Snowflake Delta Direct: https://docs.snowflake.com/en/user-guide/tables-iceberg-delta-lake
- ADR Template: https://github.com/joelparkerhenderson/architecture-decision-record

## Date

2024-01-15

## Authors

Data Engineering Team
