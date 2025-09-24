# Legacy PostgreSQL Components - DEPRECATED

**DEPRECATION NOTICE**

This directory contains legacy components of the Sales Data Aggregation Pipeline that are **DEPRECATED** and will be removed in a future version.

## Deprecation Timeline

| Phase | Date | Status | Description |
|-------|------|--------|-------------|
| **Phase 1** | 2025-Q1 | **DEPRECATED** | Components moved to legacy directory |
| **Phase 2** | 2025-Q2 | **WARNING** | Deprecation warnings added to all legacy components |
| **Phase 3** | 2025-Q3 | **END OF LIFE** | Legacy components will be removed |

## What's Deprecated

### 1. PostgreSQL-Based Pipeline Components
- **Location**: `legacy/orchestration/scripts/postgres_*.py`
- **Reason**: Replaced by modern cloud-native solutions (Snowflake, Databricks)
- **Migration Path**: See [Migration Guide](#migration-guide)

### 2. Legacy Airflow DAG
- **Location**: `legacy/orchestration/airflow/dags/legacy_postgres_pipeline_dag.py`
- **Reason**: Replaced by `cloud_sales_pipeline_dag.py` with modern orchestration
- **Migration Path**: Switch to the cloud-native DAG

### 3. PostgreSQL Infrastructure
- **Location**: `legacy/infrastructure/terraform/modules/database/`
- **Reason**: Replaced by Snowflake and cloud-native data warehousing
- **Migration Path**: Use Snowflake or Databricks infrastructure modules

## Migration Guide

### For Users Currently Using PostgreSQL Pipeline:

#### Step 1: Switch to Cloud-Native DAG
```bash
# In Airflow UI, disable the legacy DAG
# Enable the modern cloud DAG: cloud_sales_pipeline_dag.py
```

#### Step 2: Update Infrastructure
```bash
# Remove PostgreSQL infrastructure
cd infrastructure/terraform/infra
terraform destroy -target=module.database

# Deploy modern infrastructure
terraform apply
```

#### Step 3: Update Environment Configuration
```bash
# Remove PostgreSQL configs from .env
# Add Snowflake/Databricks configs
# See .env.example for modern configuration
```

#### Step 4: Data Migration (if needed)
```bash
# Export existing PostgreSQL data
python legacy/tools/export_postgres_data.py

# Import to Snowflake
python tools/import_to_snowflake.py
```

## Alternative Solutions

| Legacy Component | Modern Replacement | Benefits |
|-----------------|-------------------|----------|
| PostgreSQL Scripts | Snowflake Integration | Serverless, auto-scaling, better performance |
| RDS PostgreSQL | Snowflake Data Warehouse | Native cloud integration, columnar storage |
| Legacy DAG | Cloud Sales Pipeline DAG | Multi-cloud support, better monitoring |
| Manual ETL | Databricks Spark Jobs | Distributed processing, ML integration |

## Support Policy

### Current Support Level: **LIMITED MAINTENANCE ONLY**

- **Critical Security Fixes**: Will be applied until EOL
- **New Features**: No new features will be added
- **Bug Fixes**: Non-security bugs will not be fixed
- **Performance Improvements**: No optimizations will be made

### Getting Help

For migration assistance:
1. **Documentation**: See `docs/migration/` for detailed guides
2. **Issues**: Create GitHub issues tagged with `migration-support`
3. **Enterprise Support**: Contact your account manager

## Technical Details

### Components Included

```
legacy/
├── orchestration/
│   ├── scripts/
│   │   ├── postgres_preflight_check.py
│   │   ├── postgres_upload_data.py
│   │   ├── postgres_transform.py
│   │   ├── postgres_create_tables.py
│   │   └── postgres_load.py
│   └── airflow/
│       ├── dags/
│       │   └── legacy_postgres_pipeline_dag.py
│       └── postgres/
│           ├── postgresql.conf
│           └── postgresql.auto.conf
├── infrastructure/
│   └── terraform/
│       └── modules/
│           └── database/
│               ├── main.tf
│               ├── variables.tf
│               ├── outputs.tf
│               └── README.md
└── docs/
    └── legacy-architecture.md
```

### Known Issues

1. **Performance**: PostgreSQL-based pipeline doesn't scale for large datasets
2. **Maintenance**: Requires manual database management and maintenance
3. **Cost**: Higher operational costs compared to modern cloud solutions
4. **Integration**: Limited integration with modern ML/AI tools

## References

- [Modern Architecture Guide](../docs/architecture/modern-pipeline.md)
- [Migration Checklist](../docs/migration/checklist.md)
- [Troubleshooting Guide](../docs/migration/troubleshooting.md)
- [FAQ](../docs/migration/faq.md)

---

**Last Updated**: September 12, 2025
**Deprecation Date**: Q1 2025
**End of Life**: Q3 2025
