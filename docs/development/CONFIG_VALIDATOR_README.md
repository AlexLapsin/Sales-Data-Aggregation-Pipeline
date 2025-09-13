# Configuration Validation Framework - Setup Doctor

A comprehensive configuration validation system for the Sales Data Aggregation Pipeline that acts as a "setup doctor" to help users quickly identify and fix configuration issues.

## Overview

The configuration validation framework provides:

**Environment Variables Validation** - Format checking, required variables, template detection
**Service Connectivity Testing** - AWS, RDS, Snowflake, Kafka, Databricks
**Cross-File Consistency Checking** - Terraform, Docker Compose, dbt, Airflow DAGs
**Security Best Practices** - Network security, credentials, encryption
**Multi-Environment Support** - Dev, staging, production with different requirements
**Comprehensive Reporting** - JSON reports, CLI output, detailed suggestions
**Configuration Templates** - Pre-built templates for different scenarios

## Quick Start

### 1. Generate Configuration Template

```bash
# Generate development environment template
python config_templates.py --template dev

# Copy template to your environment file
cp .env.dev .env

# Edit .env with your actual values
```

### 2. Run Setup Doctor

```bash
# Quick health check
python setup_doctor.py

# Guided setup verification
python setup_doctor.py --check-setup

# Full validation with connectivity testing
python setup_doctor.py --full
```

### 3. Advanced Validation

```bash
# Validate specific environment
python config_validator.py --env prod

# Test connectivity to services
python config_validator.py --env prod --check-connectivity

# Generate detailed report
python config_validator.py --env prod --output report.json --verbose
```

## Tools and Scripts

### Core Validation Tools

| Script | Purpose |
|--------|---------|
| `config_validator.py` | Main validation framework with comprehensive checks |
| `setup_doctor.py` | Quick health checks and guided setup verification |
| `config_templates.py` | Generate configuration templates for different scenarios |

### Supporting Files

| File | Purpose |
|------|---------|
| `tests/test_config_validator.py` | Comprehensive test suite (500+ tests) |
| `demo_config_validator.py` | Demonstration script showing capabilities |
| `requirements-config-validator.txt` | Dependencies for validation tools |
| `CONFIG_VALIDATOR_README.md` | This documentation |

## Configuration Templates

### Available Templates

- **Minimal** (`--template minimal`) - Bare minimum configuration
- **Development** (`--template dev`) - Full development environment
- **Production** (`--template prod`) - Production-ready with security
- **Docker-only** (`--template docker`) - Local development without AWS

### Template Usage

```bash
# Generate all templates
python config_templates.py --all

# Generate specific template
python config_templates.py --template prod

# Copy and customize
cp .env.prod .env
# Edit .env with your values
```

## Validation Categories

### 1. Environment Variables

- **Required Variables**: AWS credentials, S3 buckets, project settings
- **Format Validation**: CIDR blocks, URLs, AWS key formats, regions
- **Template Detection**: Identifies placeholder values that need replacement
- **Security Checks**: Weak passwords, default credentials, exposure risks

### 2. Service Connectivity

- **AWS Services**: STS authentication, S3 bucket access
- **Database**: PostgreSQL/RDS connection testing
- **Snowflake**: Account and warehouse connectivity
- **Kafka**: Producer/consumer connectivity testing
- **Databricks**: API authentication and cluster access

### 3. Cross-File Consistency

- **Terraform**: Variable mapping between .env and variables.tf
- **Docker Compose**: Environment variable references validation
- **dbt**: Project configuration and Snowflake setup
- **Airflow DAGs**: Environment variable usage in DAG files

### 4. Security Best Practices

- **Network Security**: CIDR validation, public access warnings
- **Credentials**: Password strength, default credential detection
- **Encryption**: HTTPS enforcement, secure connection validation
- **IAM**: Role-based access recommendations

## Environment-Specific Validation

### Development Environment
- Required: AWS credentials, S3 buckets, basic project settings
- Optional: Database configuration (can use S3-only pipeline)
- Security: Moderate requirements, warnings for improvements

### Production Environment
- Required: All dev requirements + database configuration
- Security: Strict requirements, critical alerts for violations
- Network: Restricted access, no public database access
- Credentials: Strong passwords, secure connection enforcement

### Staging Environment
- Hybrid approach between dev and prod requirements
- Good for testing production-like configurations

## Usage Examples

### Basic Health Check

```bash
python setup_doctor.py
```

Output:
```
🏥 SETUP DOCTOR - Quick Health Check
==================================================
✅ .env file found
✅ Configuration looks good!
```

### Guided Setup Verification

```bash
python setup_doctor.py --check-setup
```

Output:
```
🎯 SETUP DOCTOR - Guided Setup Verification
=======================================================

📋 Environment File:
------------------------------
  ✅ .env file exists with basic variables

🔐 AWS Credentials:
------------------------------
  ✅ AWS credentials configured

🪣 S3 Configuration:
------------------------------
  ✅ S3 buckets configured
    Raw bucket: company-sales-raw-dev
    Processed bucket: company-sales-processed-dev
```

### Full Validation Report

```bash
python config_validator.py --env prod --verbose
```

Output:
```
SALES DATA PIPELINE - SETUP VALIDATION REPORT
Environment: PROD
Timestamp: 2024-01-15T10:30:00
================================================================================

SUMMARY:
Total checks: 47
✓ INFO: 23
⚠ WARNING: 8
✗ ERROR: 3
🚨 CRITICAL: 1

SECURITY.NETWORK:
-----------------
  🚨 [SECURITY_RISK] ALLOWED_CIDR is set to 0.0.0.0/0 (allows all IPs)
    💡 Restrict ALLOWED_CIDR to your specific IP or subnet
    💡 Use your public IP followed by /32 for single IP access
```

### Generate Configuration Report

```bash
python config_validator.py --env prod --output validation_report.json
```

Creates a JSON report with detailed results for integration with CI/CD systems.

## Testing

### Run Test Suite

```bash
# Install test dependencies
pip install -r requirements-config-validator.txt

# Run all tests
python -m pytest tests/test_config_validator.py -v

# Run specific test category
python -m pytest tests/test_config_validator.py::TestSecurityValidator -v
```

### Test Coverage

The test suite includes:
- 50+ unit tests for environment variable validation
- 20+ tests for connectivity testing (with mocking)
- 15+ tests for cross-file consistency checking
- 25+ tests for security validation
- 10+ integration tests for complete scenarios

### Demo Script

```bash
# Run all demos
python demo_config_validator.py --demo all

# Run specific demo
python demo_config_validator.py --demo security
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
- name: Validate Configuration
  run: |
    python config_validator.py --env prod --output validation_report.json
    if [ $? -ne 0 ]; then
      echo "Configuration validation failed"
      exit 1
    fi

- name: Upload Validation Report
  uses: actions/upload-artifact@v3
  with:
    name: config-validation-report
    path: validation_report.json
```

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: config-validation
      name: Configuration Validation
      entry: python config_validator.py --env dev
      language: system
      pass_filenames: false
```

## Troubleshooting

### Common Issues and Solutions

1. **"Missing required environment variables"**
   ```bash
   # Check which variables are missing
   python config_validator.py --env dev --verbose

   # Use a template to see all required variables
   python config_templates.py --template dev
   ```

2. **"Invalid format" errors**
   ```bash
   # Check format requirements
   python config_validator.py --env dev --verbose

   # Common fixes:
   # ALLOWED_CIDR: Use format "192.168.1.100/32"
   # AWS_DEFAULT_REGION: Use format "us-east-1"
   # DATABRICKS_HOST: Must start with "https://"
   ```

3. **"Template value detected"**
   ```bash
   # Replace placeholder values in .env:
   # Change: AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
   # To:     AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
   ```

4. **Connectivity test failures**
   ```bash
   # Install cloud libraries for connectivity testing
   pip install boto3 psycopg2-binary snowflake-connector-python kafka-python

   # Test specific services
   python config_validator.py --check-connectivity --verbose
   ```

### Debug Mode

```bash
# Run with maximum verbosity
python config_validator.py --env dev --verbose --check-connectivity

# Save detailed report for analysis
python config_validator.py --env dev --output debug_report.json --verbose
```

## Dependencies

### Core Dependencies (always required)
- Python 3.8+
- PyYAML
- ipaddress

### Optional Dependencies (for connectivity testing)
- boto3 (AWS connectivity)
- psycopg2-binary (PostgreSQL connectivity)
- snowflake-connector-python (Snowflake connectivity)
- kafka-python (Kafka connectivity)
- requests (Databricks API connectivity)

### Install All Dependencies

```bash
pip install -r requirements-config-validator.txt
```

## Architecture

### Core Components

1. **EnvironmentVariableValidator** - Validates env vars, formats, security
2. **ConnectivityTester** - Tests service connectivity and authentication
3. **ConfigurationConsistencyChecker** - Cross-file consistency validation
4. **SecurityValidator** - Security best practices enforcement
5. **SetupDoctor** - Main orchestrator and CLI interface

### Validation Flow

```
1. Load environment variables from .env file
2. Validate required variables by environment type
3. Check formats against predefined patterns
4. Test service connectivity (if requested)
5. Check cross-file configuration consistency
6. Validate security best practices
7. Generate comprehensive report with suggestions
```

### Extensibility

The framework is designed to be easily extensible:

- Add new validators by inheriting from base classes
- Add new environment types in the Environment enum
- Add new format patterns in FORMAT_PATTERNS dictionary
- Add new connectivity tests in ConnectivityTester class

## Contributing

### Adding New Validations

1. **Environment Variable Validation**
   ```python
   # Add to FORMAT_PATTERNS in EnvironmentVariableValidator
   FORMAT_PATTERNS['NEW_VAR'] = r'^pattern_here$'
   ```

2. **Service Connectivity**
   ```python
   # Add method to ConnectivityTester
   def test_new_service_connectivity(self, env_vars):
       # Implementation here
       pass
   ```

3. **Security Checks**
   ```python
   # Add method to SecurityValidator
   def _check_new_security_practice(self, env_vars):
       # Implementation here
       pass
   ```

### Testing New Features

1. Add unit tests to `tests/test_config_validator.py`
2. Add integration tests for complete scenarios
3. Update demo script to showcase new features
4. Run full test suite: `python -m pytest tests/test_config_validator.py -v`

## Support

For issues, questions, or contributions:

1. Check this documentation first
2. Run diagnostic commands:
   ```bash
   python setup_doctor.py --check-setup
   python config_validator.py --verbose
   ```
3. Generate a detailed report for analysis:
   ```bash
   python config_validator.py --output debug_report.json --verbose
   ```

The configuration validation framework provides comprehensive support for setting up and maintaining the Sales Data Aggregation Pipeline across different environments and deployment scenarios.
