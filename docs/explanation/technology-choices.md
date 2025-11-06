# Technology Choices

Rationale behind technology stack decisions for the pipeline.

## Overview

The pipeline uses a modern data stack combining open-source and cloud-native technologies:

- **Orchestration**: Apache Airflow
- **Transformation**: dbt (Data Build Tool)
- **Warehouse**: Snowflake
- **Storage**: Delta Lake on S3
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark (PySpark)
- **Infrastructure**: Terraform + Docker

## Apache Airflow

### What is Airflow

Open-source workflow orchestration platform for data pipelines.

### Why Airflow

**DAG-Based Workflow:**
- Visual representation of data dependencies
- Clear task sequencing and parallelization
- Easy debugging of failed tasks

**Extensive Integrations:**
- Built-in operators for S3, Snowflake, Spark
- Python-based custom operators
- Large ecosystem of provider packages

**Scheduling and Triggers:**
- Cron-based scheduling
- Dataset-aware triggers (new in 2.7+)
- Event-driven execution

**Monitoring:**
- Web UI for real-time status
- Task logs and metrics
- Alert integration (email, Slack, PagerDuty)

### Alternatives Considered

**Prefect:**
- Pros: Modern Python API, better local development
- Cons: Smaller ecosystem, less enterprise adoption
- Decision: Rejected due to less mature Snowflake integration

**Apache NiFi:**
- Pros: Visual drag-and-drop interface
- Cons: Heavy resource usage, steeper learning curve
- Decision: Rejected, over-engineered for this use case

**AWS Step Functions:**
- Pros: Serverless, AWS-native
- Cons: Vendor lock-in, limited local testing
- Decision: Rejected to maintain cloud portability

**Decision:** Airflow chosen for maturity, ecosystem, and dataset triggers

## dbt (Data Build Tool)

### What is dbt

SQL-first transformation framework for data warehouses.

### Why dbt

**SQL-Based:**
- Analysts can write transformations without Python
- Leverages warehouse compute (no separate Spark cluster)
- Familiar syntax for SQL developers

**Built-in Testing:**
- Schema tests (unique, not_null, relationships)
- Custom data quality tests
- Automated test execution

**Documentation:**
- Auto-generated data lineage
- Column-level descriptions
- Served as interactive website

**Incremental Models:**
```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ source('silver', 'sales') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

Only processes new data, reducing costs.

**Version Control:**
- Models are code (Git integration)
- Peer review via pull requests
- Rollback capability

### Alternatives Considered

**Apache Spark SQL:**
- Pros: Can process both Silver and Gold
- Cons: Requires Spark cluster, higher cost
- Decision: Use Spark for Silver, dbt for Gold

**Snowflake Stored Procedures:**
- Pros: Native to warehouse
- Cons: Vendor lock-in, limited testing framework
- Decision: Rejected, not portable

**Custom Python Scripts:**
- Pros: Maximum flexibility
- Cons: No testing framework, documentation, or lineage
- Decision: Rejected, not maintainable

**Decision:** dbt chosen for testing, documentation, and SQL-first approach

## Snowflake

### What is Snowflake

Cloud-native data warehouse with compute-storage separation.

### Why Snowflake

**Compute-Storage Separation:**
- Scale compute independently from storage
- Auto-suspend when idle (cost savings)
- Multiple warehouses for different workloads

**Performance:**
- Columnar storage
- Automatic clustering
- Result caching
- Materialized views

**Zero Management:**
- No infrastructure to maintain
- Automatic upgrades
- Built-in optimization

**Data Sharing:**
- Share data across accounts without copying
- Secure data clean rooms
- Marketplace integration

**Time Travel:**
- Query historical data (90 days with Enterprise)
- Restore dropped tables
- Audit changes

### Snowflake vs Alternatives

**vs Amazon Redshift:**
- Snowflake: Auto-scaling, zero-copy cloning, better concurrency
- Redshift: Cheaper for always-on workloads, tighter AWS integration
- Decision: Snowflake for auto-suspend and ease of use

**vs Google BigQuery:**
- Snowflake: Better SQL compatibility, data sharing
- BigQuery: Serverless, better ML integration
- Decision: Snowflake for enterprise features and trial credits

**vs Databricks SQL:**
- Snowflake: Purpose-built warehouse, faster queries
- Databricks: Unified data + ML platform
- Decision: Use both (Databricks for Spark, Snowflake for analytics)

**Decision:** Snowflake chosen for ease of use, auto-suspend, and trial credits

## Delta Lake

### What is Delta Lake

Open-source storage layer providing ACID transactions on data lakes.

### Why Delta Lake

**ACID Transactions:**
- Atomic commits (all-or-nothing)
- Concurrent reads and writes
- Serializable isolation
- No corrupted data from failed writes

**Schema Enforcement:**
```python
# This fails if schema mismatches
df.write.format("delta").mode("append").save(path)
```

Prevents data quality issues from schema drift.

**Time Travel:**
```python
# Read table as of version 5
df = spark.read.format("delta").option("versionAsOf", 5).load(path)

# Read table as of timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load(path)
```

**Upserts and Deletes:**
```python
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(set = {...}) \
 .whenNotMatchedInsert(values = {...}) \
 .execute()
```

Efficient CDC (Change Data Capture) processing.

**Schema Evolution:**
```python
df.write.format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .save(path)
```

Add columns without rewriting data.

### Alternatives Considered

**Apache Iceberg:**
- Pros: Vendor-neutral, multi-engine support
- Cons: Less mature, smaller ecosystem
- Decision: Rejected, Delta Lake more established

**Apache Hudi:**
- Pros: Incremental processing, record-level updates
- Cons: Complexity, steeper learning curve
- Decision: Rejected, over-engineered

**Parquet Only:**
- Pros: Simple, widely supported
- Cons: No ACID, no schema enforcement, no time travel
- Decision: Rejected, insufficient data quality guarantees

**Decision:** Delta Lake chosen for ACID, schema enforcement, and Databricks integration

## Apache Kafka

### What is Kafka

Distributed event streaming platform for real-time data pipelines.

### Why Kafka

**High Throughput:**
- Handle millions of events per second
- Horizontal scaling via partitions
- Low-latency message delivery

**Durability:**
- Events persisted to disk
- Configurable replication
- Replay capability

**Decoupling:**
```
Producers → Kafka ← Consumers
```

Producers and consumers operate independently.

**Multiple Consumers:**
- Analytics pipeline
- Real-time dashboard
- ML feature store
- Audit logger

All consume same stream without impacting each other.

**Kafka Connect:**
- Pre-built connectors (S3, Snowflake, databases)
- No custom consumer code needed
- Scalable and fault-tolerant

### Alternatives Considered

**AWS Kinesis:**
- Pros: Fully managed, AWS integration
- Cons: Vendor lock-in, limited retention (7 days)
- Decision: Rejected for portability

**Apache Pulsar:**
- Pros: Multi-tenancy, geo-replication
- Cons: Less mature, smaller ecosystem
- Decision: Rejected, Kafka more proven

**RabbitMQ:**
- Pros: Simpler setup, lower latency
- Cons: Lower throughput, not designed for streaming analytics
- Decision: Rejected, not suitable for data pipelines

**Decision:** Kafka chosen for maturity, ecosystem, and S3 sink connector

## Apache Spark

### What is Spark

Unified analytics engine for large-scale data processing.

### Why Spark

**Unified Batch and Streaming:**
```python
# Batch
df = spark.read.csv(path)

# Streaming
df = spark.readStream.json(path)

# Same transformations work for both
result = df.filter(...).groupBy(...).agg(...)
```

**Delta Lake Integration:**
- Native support for Delta format
- Optimized reads and writes
- Transaction guarantees

**Scalability:**
- Local development (`local[*]`)
- Production on Databricks (auto-scaling cluster)
- No code changes between environments

**DataFrame API:**
```python
df.filter(df.sales > 0) \
  .groupBy("category") \
  .agg(sum("sales").alias("total_sales"))
```

Familiar SQL-like syntax, compiles to optimized execution plan.

### Alternatives Considered

**Pandas:**
- Pros: Simple API, easy to learn
- Cons: Single-machine only, memory-bound
- Decision: Not suitable for large datasets

**Apache Flink:**
- Pros: True streaming (not micro-batches)
- Cons: Steeper learning curve, less mature
- Decision: Rejected, Spark sufficient for 5-minute latency

**AWS Glue:**
- Pros: Serverless, managed
- Cons: Limited debugging, slower cold starts
- Decision: Use Spark on Databricks for better control

**Decision:** Spark chosen for unified batch/streaming and Delta Lake support

## Terraform

### What is Terraform

Infrastructure as Code (IaC) tool for provisioning cloud resources.

### Why Terraform

**Declarative Syntax:**
```hcl
resource "aws_s3_bucket" "raw" {
  bucket = var.RAW_BUCKET
}
```

Declare desired state, Terraform handles creation/updates.

**Multi-Cloud:**
- AWS (S3, IAM)
- Snowflake (database, warehouse)
- Single tool for entire stack

**State Management:**
- Tracks deployed resources
- Detects drift from desired state
- Safe updates (plan before apply)

**Modules:**
```
modules/
├── s3/        # Reusable S3 bucket module
├── iam/       # Reusable IAM role module
└── snowflake/ # Reusable Snowflake module
```

**Version Control:**
- Infrastructure defined in Git
- Peer review via pull requests
- Audit trail of changes

### Alternatives Considered

**AWS CloudFormation:**
- Pros: Native AWS integration
- Cons: AWS-only, YAML verbosity
- Decision: Rejected, need Snowflake support

**Pulumi:**
- Pros: Use real programming languages
- Cons: Smaller ecosystem, less mature
- Decision: Rejected, Terraform more established

**Manual Setup:**
- Pros: Simple for small deployments
- Cons: Not reproducible, error-prone
- Decision: Rejected, not scalable

**Decision:** Terraform chosen for multi-cloud support and declarative syntax

## Docker

### What is Docker

Platform for containerizing applications.

### Why Docker

**Consistent Environments:**
- Same environment in dev, staging, prod
- No "works on my machine" issues
- Dependency conflicts resolved

**Rapid Setup:**
```bash
docker-compose up -d
```

Entire stack running in 5 minutes.

**Isolation:**
- Airflow containers separate from Kafka
- Version conflicts avoided
- Easy to tear down and rebuild

**Portability:**
- Run on any OS (Windows, Mac, Linux)
- Move from laptop to cloud seamlessly

### Alternatives Considered

**Virtual Machines:**
- Pros: Complete OS isolation
- Cons: Heavy resource usage, slow startup
- Decision: Rejected, containers sufficient

**Native Installation:**
- Pros: Maximum performance
- Cons: Complex setup, dependency conflicts
- Decision: Rejected, not portable

**Kubernetes:**
- Pros: Production-grade orchestration
- Cons: Over-engineered for development
- Decision: Use Docker Compose for dev, Kubernetes for prod (future)

**Decision:** Docker Compose for development, Kubernetes consideration for production

## Cost Optimization

### Development

**Free Tier Usage:**
- AWS: Free tier S3 (first 5GB)
- Snowflake: 30-day trial with $400 credits
- Databricks: Community edition (limited)

**Cost:** $0-10/month

### Production (Estimated)

| Service | Monthly Cost |
|---------|--------------|
| AWS S3 (Bronze + Silver) | $30 |
| Snowflake (100 queries/day) | $50 |
| Databricks (10 hours/month) | $40 |
| AWS MSK (Kafka) | $75 |
| **Total** | **$195/month** |

Optimizations:
- Snowflake auto-suspend: Saves 80% compute
- S3 lifecycle policies: Move Bronze to Glacier after 90 days
- Databricks auto-terminate: Only run when needed
- Spot instances for Spark: 70% cost savings

**Optimized Total:** $80-100/month

## Future Considerations

**MLOps:**
- Add MLflow for model tracking
- Feature store on Delta Lake
- Model serving on Databricks

**Data Quality:**
- Great Expectations for advanced testing
- Monte Carlo for data observability
- dbt Elementary for monitoring

**Governance:**
- Apache Atlas for metadata management
- Collibra for data catalog
- Snowflake tags for data classification

**Real-Time:**
- Apache Flink for true streaming
- ksqlDB for stream processing
- Materialize for incremental views

## Conclusion

Technology choices prioritize:

1. **Reliability**: ACID transactions, data quality testing
2. **Scalability**: Cloud-native, horizontal scaling
3. **Cost**: Auto-suspend, lifecycle policies, open-source where possible
4. **Flexibility**: Multi-cloud, portable, extensible
5. **Developer Experience**: SQL-first, visual interfaces, rapid iteration

The stack balances cutting-edge technologies (Delta Lake, Dataset triggers) with proven enterprise standards (Airflow, Snowflake) to create a production-ready yet maintainable data pipeline.
