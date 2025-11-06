#!/usr/bin/env python3
"""
Configuration Templates Generator

This script generates configuration templates and examples for different
deployment scenarios of the Sales Data Aggregation Pipeline.

Usage:
    python config_templates.py --template dev
    python config_templates.py --template prod
    python config_templates.py --template minimal
    python config_templates.py --all
"""

import argparse
import os
from pathlib import Path
from typing import Dict, Any


class ConfigTemplateGenerator:
    """Generates configuration templates for different scenarios"""

    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()

    def generate_dev_template(self) -> str:
        """Generate development environment template"""
        return """# =============================
# DEVELOPMENT ENVIRONMENT CONFIGURATION
# =============================
# Copy this to .env and customize with your values

# =============================
# AWS CREDENTIALS (required)
# =============================
# Get these from AWS IAM console
AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION="us-east-1"

# =============================
# DATA LAKE BUCKETS (required)
# =============================
# These must be globally unique
RAW_BUCKET="your-company-sales-raw-dev"
PROCESSED_BUCKET="your-company-sales-processed-dev"
S3_PREFIX=""

# =============================
# PROJECT CONFIGURATION (required)
# =============================
PROJECT_NAME="sales-data-pipeline"
ENVIRONMENT="dev"
# Your public IP with /32 suffix (get with: curl ifconfig.me)
ALLOWED_CIDR="YOUR.PUBLIC.IP.ADDR/32"

# =============================
# DOCKER CONFIGURATION (required)
# =============================
# Full path to your project directory
HOST_REPO_ROOT="/full/path/to/your/project"
HOST_DATA_DIR="/full/path/to/your/project/data"
PIPELINE_IMAGE="sales-pipeline:latest"

# =============================
# PIPELINE SETTINGS
# =============================
SALES_THRESHOLD=10000

# =============================
# AIRFLOW SETTINGS
# =============================
_AIRFLOW_WWW_USER_USERNAME="admin"
_AIRFLOW_WWW_USER_PASSWORD="dev_password"
_AIRFLOW_WWW_USER_EMAIL="admin@yourcompany.com"
OWNER_NAME="data-team"
ALERT_EMAIL="alerts@yourcompany.com"

# =============================
# TERRAFORM SETTINGS (for infrastructure)
# =============================
TRUSTED_PRINCIPAL_ARN="arn:aws:iam::YOUR_ACCOUNT:user/YOUR_USER"

# =============================
# OPTIONAL CLOUD SERVICES
# =============================

# Kafka Configuration (for streaming)
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
KAFKA_TOPIC="sales_events"
PRODUCER_INTERVAL="1.0"
PRODUCER_BATCH_SIZE="16384"
PRODUCER_COMPRESSION="gzip"

# MSK Configuration (optional)
ENABLE_MSK="false"
KAFKA_VERSION="2.8.1"
BROKER_INSTANCE_TYPE="kafka.t3.small"
NUMBER_OF_BROKERS="3"

# Snowflake Configuration (optional)
ENABLE_SNOWFLAKE_OBJECTS="false"
SNOWFLAKE_ACCOUNT="YOUR_ACCOUNT.us-east-1.aws"
SNOWFLAKE_USER="YOUR_USERNAME"
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"
SNOWFLAKE_ROLE="SYSADMIN"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_DATABASE="SALES_DW"
SNOWFLAKE_SCHEMA="RAW"
SNOWFLAKE_REGION="us-east-1"

# Databricks Configuration (optional)
DATABRICKS_HOST="https://YOUR_WORKSPACE.cloud.databricks.com"
DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN"
DATABRICKS_CLUSTER_ID="YOUR_CLUSTER_ID"
"""

    def generate_prod_template(self) -> str:
        """Generate production environment template"""
        return """# =============================
# PRODUCTION ENVIRONMENT CONFIGURATION
# =============================
# Use this template for production deployments
# SECURITY NOTE: Ensure this file is never committed to version control

# =============================
# AWS CREDENTIALS (required)
# =============================
# Use IAM roles in production instead of access keys when possible
AWS_ACCESS_KEY_ID="YOUR_PROD_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY="YOUR_PROD_AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION="us-east-1"

# =============================
# DATA LAKE BUCKETS (required)
# =============================
# Use company naming convention
RAW_BUCKET="company-sales-raw-prod"
PROCESSED_BUCKET="company-sales-processed-prod"
S3_PREFIX=""

# =============================
# PROJECT CONFIGURATION (required)
# =============================
PROJECT_NAME="sales-data-pipeline"
ENVIRONMENT="prod"
# Restrict to your corporate network
ALLOWED_CIDR="10.0.0.0/16"

# =============================
# DOCKER CONFIGURATION
# =============================
HOST_REPO_ROOT="/opt/sales-pipeline"
HOST_DATA_DIR="/app/data"
PIPELINE_IMAGE="your-registry/sales-pipeline:v1.0.0"

# =============================
# PIPELINE SETTINGS
# =============================
SALES_THRESHOLD=50000

# =============================
# AIRFLOW SETTINGS
# =============================
_AIRFLOW_WWW_USER_USERNAME="admin"
_AIRFLOW_WWW_USER_PASSWORD="SecureAirflowPassword123!"
_AIRFLOW_WWW_USER_EMAIL="admin@yourcompany.com"
OWNER_NAME="data-engineering"
ALERT_EMAIL="data-alerts@yourcompany.com"

# =============================
# TERRAFORM SETTINGS (production grade)
# =============================
TRUSTED_PRINCIPAL_ARN="arn:aws:iam::PROD_ACCOUNT:role/SalesDataPipelineRole"

# =============================
# CLOUD SERVICES (production)
# =============================

# MSK Configuration (managed Kafka)
ENABLE_MSK="true"
KAFKA_VERSION="2.8.1"
BROKER_INSTANCE_TYPE="kafka.m5.large"
NUMBER_OF_BROKERS="3"
KAFKA_BOOTSTRAP_SERVERS="kafka-cluster.msk.us-east-1.amazonaws.com:9092"
KAFKA_TOPIC="sales_events"

# Snowflake Configuration (data warehouse)
ENABLE_SNOWFLAKE_OBJECTS="true"
SNOWFLAKE_ACCOUNT="yourcompany.us-east-1.aws"
SNOWFLAKE_USER="PIPELINE_USER"
SNOWFLAKE_PASSWORD="SecureSnowflakePassword123!"
SNOWFLAKE_ROLE="DATA_ENGINEER"
SNOWFLAKE_WAREHOUSE="PROD_WH"
SNOWFLAKE_DATABASE="SALES_DW"
SNOWFLAKE_SCHEMA="PROD"
SNOWFLAKE_REGION="us-east-1"

# Databricks Configuration (data processing)
DATABRICKS_HOST="https://yourcompany.cloud.databricks.com"
DATABRICKS_TOKEN="YOUR_DATABRICKS_ACCESS_TOKEN"
DATABRICKS_CLUSTER_ID="0123-456789-abcde123"

# =============================
# MONITORING AND ALERTING
# =============================
SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
GRAFANA_PASSWORD="SecureGrafanaPassword123!"
MONITOR_INTERVAL="300"
"""

    def generate_minimal_template(self) -> str:
        """Generate minimal working configuration"""
        return """# =============================
# MINIMAL CONFIGURATION
# =============================
# Bare minimum settings to get started

# AWS Credentials (required)
AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION="us-east-1"

# S3 Buckets (required - must be globally unique)
RAW_BUCKET="your-unique-bucket-name-raw"
PROCESSED_BUCKET="your-unique-bucket-name-processed"

# Project Settings (required)
PROJECT_NAME="sales-pipeline"
ENVIRONMENT="dev"
ALLOWED_CIDR="YOUR.IP.ADDRESS/32"

# Docker Paths (required)
HOST_DATA_DIR="/full/path/to/your/project/data"
PIPELINE_IMAGE="sales-pipeline:latest"
"""

    def generate_docker_env_template(self) -> str:
        """Generate template for Docker-only deployment"""
        return """# =============================
# DOCKER-ONLY CONFIGURATION
# =============================
# For local development without AWS infrastructure

# Local Services
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow"
POSTGRES_DB="airflow"

# Kafka (local)
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
KAFKA_TOPIC="sales_events"

# Airflow
_AIRFLOW_WWW_USER_USERNAME="admin"
_AIRFLOW_WWW_USER_PASSWORD="admin"
_AIRFLOW_WWW_USER_EMAIL="admin@example.com"

# Pipeline Settings
PIPELINE_IMAGE="sales-pipeline:latest"
HOST_DATA_DIR="/full/path/to/your/project/data"
SALES_THRESHOLD="10000"

# Mock AWS (for testing)
AWS_ACCESS_KEY_ID="minioadmin"
AWS_SECRET_ACCESS_KEY="minioadmin"
AWS_DEFAULT_REGION="us-east-1"
RAW_BUCKET="local-raw-bucket"
PROCESSED_BUCKET="local-processed-bucket"

# Optional: Use LocalStack for AWS simulation
# LOCALSTACK_ENDPOINT="http://localhost:4566"
"""

    def save_template(self, template_name: str, content: str):
        """Save template to file"""
        filename = f".env.{template_name}"
        filepath = self.project_root / filename

        with open(filepath, "w") as f:
            f.write(content)

        print(f"✓ Generated template: {filename}")
        return filepath

    def generate_setup_instructions(self) -> str:
        """Generate setup instructions"""
        return """# SALES DATA PIPELINE - SETUP INSTRUCTIONS

## Quick Start Guide

### 1. Choose Your Configuration Template

We provide several configuration templates for different use cases:

- **Minimal** (`python config_templates.py --template minimal`)
  - Bare minimum configuration to get started
  - S3-based pipeline without database
  - Good for testing and learning

- **Development** (`python config_templates.py --template dev`)
  - Full development environment
  - Includes all optional services
  - Good for feature development

- **Production** (`python config_templates.py --template prod`)
  - Production-ready configuration
  - Enhanced security settings
  - Managed cloud services

- **Docker-only** (`python config_templates.py --template docker`)
  - Local development without AWS
  - Uses local services only
  - Good for offline development

### 2. Configure Your Environment

1. Choose and generate a template:
   ```bash
   python config_templates.py --template dev
   ```

2. Copy the template to your environment file:
   ```bash
   cp .env.dev .env
   ```

3. Edit `.env` with your actual values:
   ```bash
   # Required: Replace template values
   AWS_ACCESS_KEY_ID="your_actual_access_key"
   AWS_SECRET_ACCESS_KEY="your_actual_secret_key"
   RAW_BUCKET="your-unique-bucket-name"
   ALLOWED_CIDR="your.ip.address/32"
   HOST_DATA_DIR="/full/path/to/your/project/data"
   ```

### 3. Validate Your Configuration

Run the setup doctor to validate your configuration:

```bash
# Quick health check
python setup_doctor.py

# Full validation with connectivity tests
python setup_doctor.py --full

# Guided setup verification
python setup_doctor.py --check-setup
```

### 4. Deploy Infrastructure (if using AWS)

```bash
# Export environment variables for Terraform
source export_tf_vars.sh

# Initialize and apply Terraform
cd infra
terraform init
terraform apply
```

### 5. Start the Pipeline

```bash
# Build and start services
docker-compose up --build -d

# Access Airflow UI
open http://localhost:8080
```

## Configuration Variables Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `wJal...` |
| `AWS_DEFAULT_REGION` | AWS region | `us-east-1` |
| `RAW_BUCKET` | Raw data bucket | `company-sales-raw` |
| `PROCESSED_BUCKET` | Processed data bucket | `company-sales-processed` |
| `PROJECT_NAME` | Project identifier | `sales-pipeline` |
| `ENVIRONMENT` | Environment name | `dev`, `staging`, `prod` |
| `ALLOWED_CIDR` | Network access CIDR | `192.168.1.100/32` |
| `HOST_DATA_DIR` | Local data directory | `/path/to/project/data` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account | (optional) |
| `DATABRICKS_HOST` | Databricks workspace | (optional) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | `localhost:9092` |
| `SALES_THRESHOLD` | Sales filtering threshold | `10000` |

### Security Best Practices

1. **Network Security**
   - Use specific CIDR blocks, not `0.0.0.0/0`
   - Use VPN or private networks for production access

2. **Credentials Management**
   - Use strong passwords (8+ characters, mixed case, numbers, symbols)
   - Consider AWS Secrets Manager for production
   - Never commit `.env` files to version control

3. **IAM Best Practices**
   - Use IAM roles instead of access keys when possible
   - Configure `TRUSTED_PRINCIPAL_ARN` for role-based access
   - Follow principle of least privilege

## Troubleshooting

### Common Issues

1. **"Bucket name not available"**
   - S3 bucket names must be globally unique
   - Try adding random suffix: `company-sales-raw-12345`

2. **"Access denied" errors**
   - Check AWS credentials and permissions
   - Verify IAM policies allow required actions

3. **Docker permission errors**
   - Ensure Docker daemon is running
   - Check file permissions on `HOST_DATA_DIR`

### Getting Help

1. Run configuration validation:
   ```bash
   python config_validator.py --verbose
   ```

2. Check setup doctor:
   ```bash
   python setup_doctor.py --check-setup
   ```

3. View detailed logs:
   ```bash
   docker-compose logs [service-name]
   ```

4. Generate validation report:
   ```bash
   python config_validator.py --output report.json
   ```
"""

    def generate_all_templates(self):
        """Generate all configuration templates"""
        templates = {
            "minimal": self.generate_minimal_template(),
            "dev": self.generate_dev_template(),
            "prod": self.generate_prod_template(),
            "docker": self.generate_docker_env_template(),
        }

        print("Generating configuration templates...")
        print("=" * 50)

        generated_files = []
        for name, content in templates.items():
            filepath = self.save_template(name, content)
            generated_files.append(filepath)

        # Generate setup instructions
        instructions = self.generate_setup_instructions()
        instructions_file = self.project_root / "SETUP_INSTRUCTIONS.md"
        with open(instructions_file, "w") as f:
            f.write(instructions)
        print(f"✓ Generated setup instructions: SETUP_INSTRUCTIONS.md")
        generated_files.append(instructions_file)

        print(f"\nGenerated {len(generated_files)} files:")
        for filepath in generated_files:
            print(f"   - {filepath.name}")

        print(f"\nNext steps:")
        print(f"   1. Copy a template: cp .env.dev .env")
        print(f"   2. Edit .env with your values")
        print(f"   3. Run: python setup_doctor.py")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Generate configuration templates for Sales Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python config_templates.py --template dev     # Generate development template
  python config_templates.py --template prod    # Generate production template
  python config_templates.py --all              # Generate all templates
        """,
    )

    parser.add_argument(
        "--template",
        choices=["minimal", "dev", "prod", "docker"],
        help="Generate specific template",
    )

    parser.add_argument(
        "--all", action="store_true", help="Generate all templates and instructions"
    )

    parser.add_argument(
        "--project-root",
        type=str,
        help="Project root directory (default: current directory)",
    )

    args = parser.parse_args()

    if not args.template and not args.all:
        parser.print_help()
        return

    generator = ConfigTemplateGenerator(args.project_root)

    if args.all:
        generator.generate_all_templates()
    elif args.template:
        template_methods = {
            "minimal": generator.generate_minimal_template,
            "dev": generator.generate_dev_template,
            "prod": generator.generate_prod_template,
            "docker": generator.generate_docker_env_template,
        }

        content = template_methods[args.template]()
        generator.save_template(args.template, content)

        print(f"\nNext steps:")
        print(f"   1. Copy template: cp .env.{args.template} .env")
        print(f"   2. Edit .env with your values")
        print(f"   3. Run validation: python setup_doctor.py")


if __name__ == "__main__":
    main()
