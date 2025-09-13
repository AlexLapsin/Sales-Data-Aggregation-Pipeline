# Migration Checklist: PostgreSQL to Modern Cloud Pipeline

## Overview

This checklist helps you migrate from the deprecated PostgreSQL-based sales data aggregation pipeline to the modern cloud-native solution.

## Pre-Migration Assessment

### ✅ Current State Analysis
- [ ] **Identify Current Usage**: Document which PostgreSQL components you're currently using
- [ ] **Data Volume Assessment**: Measure your current data volume and processing requirements
- [ ] **Integration Points**: List all systems that integrate with your current PostgreSQL pipeline
- [ ] **Performance Requirements**: Document current SLAs and performance expectations
- [ ] **Security Requirements**: Review current security and compliance needs

### ✅ Environment Preparation
- [ ] **Cloud Credentials**: Ensure you have necessary AWS credentials and permissions
- [ ] **Snowflake Access** (if choosing Snowflake): Set up Snowflake account and credentials
- [ ] **Databricks Access** (if choosing Databricks): Set up Databricks workspace and tokens
- [ ] **Network Configuration**: Verify network connectivity and security groups
- [ ] **Backup Strategy**: Plan for data backup during migration

## Migration Path Selection

Choose your migration path based on your requirements:

### 🌟 Option A: Full Modern Pipeline (Recommended)
**Best for**: New deployments, high scalability needs, ML integration requirements

- [ ] **Target Architecture**: Snowflake + Databricks + Kafka
- [ ] **Benefits**: Best performance, auto-scaling, ML-ready
- [ ] **Effort**: Medium to High
- [ ] **Timeline**: 2-4 weeks

### 🔄 Option B: Hybrid Approach
**Best for**: Gradual migration, risk-averse organizations

- [ ] **Target Architecture**: Keep some PostgreSQL + Add modern components
- [ ] **Benefits**: Lower risk, gradual transition
- [ ] **Effort**: Medium
- [ ] **Timeline**: 4-8 weeks

### 📦 Option C: Legacy Maintenance
**Best for**: Short-term maintenance, minimal changes required

- [ ] **Target Architecture**: Use legacy components from `legacy/` directory
- [ ] **Benefits**: Minimal changes required
- [ ] **Effort**: Low
- [ ] **Timeline**: 1 week
- [ ] **⚠️ NOTE**: Limited support, will be removed in Q3 2025

## Step-by-Step Migration

### Phase 1: Infrastructure Setup

#### For Modern Pipeline (Option A):
- [ ] **Deploy Snowflake Infrastructure**:
  ```bash
  cd infrastructure/terraform/infra
  terraform init
  export ENABLE_SNOWFLAKE_OBJECTS=true
  terraform apply -target=module.snowflake
  ```

- [ ] **Deploy Kafka Infrastructure**:
  ```bash
  export ENABLE_MSK=true
  terraform apply -target=module.kafka
  ```

- [ ] **Configure Environment Variables**:
  ```bash
  cp .env.example .env
  # Update .env with Snowflake/Databricks credentials
  source export_tf_vars.sh
  ```

#### For Legacy Maintenance (Option C):
- [ ] **Deploy Legacy Infrastructure**:
  ```bash
  cd legacy/infrastructure/terraform
  terraform init
  terraform apply
  ```

### Phase 2: Pipeline Configuration

#### For Modern Pipeline:
- [ ] **Enable Cloud DAG**:
  - Access Airflow UI (http://localhost:8080)
  - Enable `cloud_sales_pipeline_dag`
  - Disable `sales_data_aggregation` (deprecated)

- [ ] **Configure Data Sources**:
  ```bash
  # Update data source configurations
  # See docs/configuration/data-sources.md
  ```

- [ ] **Test Pipeline**:
  ```bash
  # Trigger a test run in Airflow UI
  # Monitor logs for successful execution
  ```

#### For Legacy Maintenance:
- [ ] **Enable Legacy DAG**:
  - Access Airflow UI
  - Enable `legacy_postgres_sales_aggregation`
  - Disable `sales_data_aggregation`

### Phase 3: Data Migration

#### For Modern Pipeline:
- [ ] **Export Existing Data**:
  ```bash
  python tools/export_postgres_data.py \
    --output-format parquet \
    --target-location s3://your-bucket/migration/
  ```

- [ ] **Import to Modern System**:
  ```bash
  # For Snowflake
  python tools/import_to_snowflake.py \
    --source s3://your-bucket/migration/ \
    --target-table sales_data

  # For Databricks
  python spark/migrate_data.py \
    --source s3://your-bucket/migration/
  ```

- [ ] **Verify Data Integrity**:
  ```bash
  python tools/verify_migration.py \
    --source postgresql \
    --target snowflake
  ```

### Phase 4: Testing & Validation

- [ ] **Functional Testing**:
  ```bash
  pytest tests/integration/test_modern_pipeline.py
  ```

- [ ] **Performance Testing**:
  ```bash
  python tools/performance_benchmark.py \
    --pipeline modern \
    --data-volume large
  ```

- [ ] **Data Quality Validation**:
  ```bash
  # Run data quality tests
  dbt test --models marts
  ```

- [ ] **End-to-End Testing**:
  - [ ] Trigger full pipeline run
  - [ ] Verify output data quality
  - [ ] Check monitoring and alerting
  - [ ] Validate downstream integrations

### Phase 5: Cutover & Cleanup

- [ ] **Production Cutover**:
  - [ ] Schedule maintenance window
  - [ ] Update DNS/load balancer configurations
  - [ ] Switch traffic to new pipeline
  - [ ] Monitor for issues

- [ ] **Legacy Cleanup** (For Modern Pipeline):
  - [ ] **Remove Legacy Infrastructure**:
    ```bash
    cd legacy/infrastructure/terraform
    terraform destroy
    ```
  - [ ] **Archive Legacy Data**:
    ```bash
    python tools/archive_legacy_data.py
    ```
  - [ ] **Update Documentation**:
    - [ ] Update runbooks
    - [ ] Update monitoring dashboards
    - [ ] Update team documentation

## Post-Migration Verification

### ✅ Technical Validation
- [ ] **Pipeline Execution**: Verify pipeline runs successfully end-to-end
- [ ] **Data Quality**: Compare data quality metrics with legacy system
- [ ] **Performance**: Validate performance meets or exceeds requirements
- [ ] **Monitoring**: Ensure all monitoring and alerting is working
- [ ] **Security**: Verify security controls are properly implemented

### ✅ Operational Validation
- [ ] **Team Training**: Ensure team is trained on new system operations
- [ ] **Documentation**: Update all operational documentation
- [ ] **Runbooks**: Create/update incident response runbooks
- [ ] **Backup/Recovery**: Validate backup and recovery procedures
- [ ] **Business Continuity**: Ensure business continuity plans are updated

## Rollback Plan

In case issues are encountered:

### 🚨 Emergency Rollback
- [ ] **Immediate Actions**:
  ```bash
  # Re-enable legacy DAG
  # Redirect traffic back to legacy infrastructure
  # Notify stakeholders
  ```

- [ ] **Data Recovery**:
  ```bash
  # Restore from backup if necessary
  # Validate data integrity
  ```

## Timeline & Milestones

| Phase | Duration | Key Deliverables |
|-------|----------|------------------|
| **Phase 1: Infrastructure** | 3-5 days | Modern infrastructure deployed and tested |
| **Phase 2: Pipeline Config** | 2-3 days | Pipeline configured and basic functionality verified |
| **Phase 3: Data Migration** | 3-7 days | All historical data migrated and validated |
| **Phase 4: Testing** | 5-10 days | Comprehensive testing completed |
| **Phase 5: Cutover** | 1-2 days | Production cutover completed |

**Total Estimated Duration**: 2-4 weeks (depending on chosen option and complexity)

## Support & Resources

### 📞 Getting Help
- **Technical Issues**: Create GitHub issue with `migration-support` tag
- **Architecture Questions**: Contact the platform team
- **Urgent Issues**: Follow escalation procedure in runbooks

### 📚 Additional Resources
- [Migration FAQ](faq.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Modern Architecture Overview](../architecture/modern-pipeline.md)
- [Performance Tuning Guide](../operations/performance-tuning.md)

### 🔧 Migration Tools
- `tools/export_postgres_data.py` - Export PostgreSQL data
- `tools/import_to_snowflake.py` - Import data to Snowflake
- `tools/verify_migration.py` - Verify data integrity
- `tools/performance_benchmark.py` - Performance testing

---

**Last Updated**: September 12, 2025
**Version**: 1.0
**Next Review**: October 12, 2025
