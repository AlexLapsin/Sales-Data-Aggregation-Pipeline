# Sales Data Aggregation Pipeline - Documentation

Enterprise data pipeline demonstrating Medallion architecture with Apache Airflow, dbt, Apache Kafka, PySpark, Delta Lake, and Snowflake.

## Getting Started

**New to this project?** Follow the [Getting Started Tutorial](tutorial/getting-started.md) - complete walkthrough from zero to running pipeline (60 min).

## How-To Guides

Practical guides for specific tasks:

- [Deploy Infrastructure](how-to/deploy-infrastructure.md) - Terraform setup for AWS and Snowflake
- [Configure Environment](how-to/configure-environment.md) - Set up local development environment
- [Run Batch Pipeline](how-to/run-batch-pipeline.md) - Execute CSV ingestion and processing
- [Run Streaming Pipeline](how-to/run-streaming-pipeline.md) - Execute real-time Kafka data flow
- [Troubleshooting](how-to/troubleshooting.md) - Common issues and solutions

## Reference

Technical specifications and detailed information:

- [Architecture](reference/architecture.md) - System design and component overview
- [Data Dictionary](reference/data-dictionary.md) - Schema definitions and field descriptions
- [Configuration](reference/configuration.md) - All environment variables and settings
- [CLI Reference](reference/cli-reference.md) - Command-line tools and options

## Explanation

Concepts, design decisions, and rationale:

- [Medallion Architecture](explanation/medallion-architecture.md) - Why Bronze/Silver/Gold layers
- [Technology Choices](explanation/technology-choices.md) - Why Airflow, dbt, Snowflake, Delta Lake
- [Architecture Decision Records](explanation/adr/) - Historical design decisions

---

## Archive

Historical documentation available in [`archive/`](archive/) directory.
