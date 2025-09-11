#!/usr/bin/env python3
"""
Setup Doctor - Quick Configuration Validation Script

This is a convenience script that provides common validation scenarios
for the Sales Data Aggregation Pipeline. It demonstrates how to use
the configuration validator and provides quick health checks.

Usage:
    python setup_doctor.py                    # Quick dev environment check
    python setup_doctor.py --full             # Complete validation with connectivity
    python setup_doctor.py --prod             # Production environment validation
    python setup_doctor.py --check-setup      # Guided setup verification
"""

import sys
import os
import argparse
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config_validator import SetupDoctor, Environment


def quick_health_check():
    """Quick health check for development environment"""
    print("SETUP DOCTOR - Quick Health Check")
    print("=" * 50)

    doctor = SetupDoctor()

    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        print("X .env file not found!")
        print("→ Copy .env.example to .env and configure your settings")
        return False

    print("✓ .env file found")

    # Run basic validation
    try:
        report = doctor.run_validation(Environment.DEV, check_connectivity=False)

        if report.has_errors():
            print(f"X Configuration has {report.summary.get('error', 0)} errors")
            print("→ Run with --verbose for details")
            return False
        elif report.has_warnings():
            print(f"⚠ Configuration has {report.summary.get('warning', 0)} warnings")
            print("✓ Ready for development (with warnings)")
            return True
        else:
            print("✓ Configuration looks good!")
            return True

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def full_validation(environment: str = "dev"):
    """Full validation with connectivity testing"""
    print(f"SETUP DOCTOR - Full Validation ({environment.upper()})")
    print("=" * 60)

    doctor = SetupDoctor()
    env = Environment(environment)

    try:
        report = doctor.run_validation(env, check_connectivity=True)
        doctor.print_report(report, verbose=True)

        # Save report
        report_file = f"validation_report_{environment}.json"
        doctor.save_report(report, report_file)
        print(f"\nDetailed report saved to: {report_file}")

        return not report.has_errors()

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def guided_setup_check():
    """Guided setup verification with step-by-step checks"""
    print("SETUP DOCTOR - Guided Setup Verification")
    print("=" * 55)

    steps = [
        ("Environment File", check_env_file),
        ("AWS Credentials", check_aws_credentials),
        ("S3 Configuration", check_s3_config),
        ("Database Configuration", check_database_config),
        ("Docker Configuration", check_docker_config),
        ("Terraform Configuration", check_terraform_config),
    ]

    results = {}

    for step_name, check_func in steps:
        print(f"\n{step_name}:")
        print("-" * 30)
        try:
            result = check_func()
            results[step_name] = result
            if result:
                print("✓ PASSED")
            else:
                print("❌ NEEDS ATTENTION")
        except Exception as e:
            print(f"❌ ERROR: {e}")
            results[step_name] = False

    # Summary
    print(f"\n{'='*55}")
    print("SETUP VERIFICATION SUMMARY")
    print(f"{'='*55}")

    passed = sum(1 for r in results.values() if r)
    total = len(results)

    for step, result in results.items():
        status = "✓" if result else "X"
        print(f"{status} {step}")

    print(f"\nOverall: {passed}/{total} checks passed")

    if passed == total:
        print("Setup verification completed successfully!")
        print("You're ready to run the pipeline!")
    else:
        print("Some setup issues need to be addressed.")
        print("→ Run 'python config_validator.py --verbose' for detailed guidance")

    return passed == total


def check_env_file():
    """Check if .env file exists and has basic content"""
    env_file = Path(".env")
    if not env_file.exists():
        print("  ❌ .env file not found")
        print("  → Copy .env.example to .env")
        return False

    # Check for basic required variables
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET"]

    with open(env_file, "r") as f:
        content = f.read()

    missing_vars = []
    for var in required_vars:
        if f"{var}=" not in content:
            missing_vars.append(var)

    if missing_vars:
        print(f"  ⚠ Missing variables: {', '.join(missing_vars)}")
        return False

    print("  ✓ .env file exists with basic variables")
    return True


def check_aws_credentials():
    """Check AWS credentials configuration"""
    doctor = SetupDoctor()
    env_vars = doctor.load_environment_variables()

    aws_key = env_vars.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = env_vars.get("AWS_SECRET_ACCESS_KEY", "")
    aws_region = env_vars.get("AWS_DEFAULT_REGION", "")

    issues = []

    if not aws_key or aws_key.startswith("YOUR_"):
        issues.append("AWS_ACCESS_KEY_ID not configured")

    if not aws_secret or aws_secret.startswith("YOUR_"):
        issues.append("AWS_SECRET_ACCESS_KEY not configured")

    if not aws_region:
        issues.append("AWS_DEFAULT_REGION not set")

    if issues:
        for issue in issues:
            print(f"  ❌ {issue}")
        print("  → Configure your AWS credentials in .env")
        return False

    print("  ✓ AWS credentials configured")
    return True


def check_s3_config():
    """Check S3 bucket configuration"""
    doctor = SetupDoctor()
    env_vars = doctor.load_environment_variables()

    s3_bucket = env_vars.get("S3_BUCKET", "")
    processed_bucket = env_vars.get("PROCESSED_BUCKET", "")

    issues = []

    if not s3_bucket or s3_bucket.startswith("RAW_BUCKET"):
        issues.append("S3_BUCKET not configured")

    if not processed_bucket or processed_bucket.startswith("PROCESSED_BUCKET"):
        issues.append("PROCESSED_BUCKET not configured")

    if issues:
        for issue in issues:
            print(f"  ❌ {issue}")
        print("  → Set unique S3 bucket names in .env")
        return False

    print("  ✓ S3 buckets configured")
    print(f"    Raw bucket: {s3_bucket}")
    print(f"    Processed bucket: {processed_bucket}")
    return True


def check_database_config():
    """Check database configuration"""
    doctor = SetupDoctor()
    env_vars = doctor.load_environment_variables()

    rds_host = env_vars.get("RDS_HOST", "")
    rds_user = env_vars.get("RDS_USER", "")
    rds_pass = env_vars.get("RDS_PASS", "")

    if not rds_host:
        print("  ⚠ RDS_HOST not set (OK for initial setup)")
        print("  → Set after running 'terraform apply'")
        return True

    issues = []

    if rds_pass == "CHANGE_ME_STRONG_PASSWORD":
        issues.append("RDS_PASS uses default value")

    if not rds_user:
        issues.append("RDS_USER not set")

    if issues:
        for issue in issues:
            print(f"  ❌ {issue}")
        return False

    print("  ✓ Database configuration looks good")
    return True


def check_docker_config():
    """Check Docker-related configuration"""
    # Check if Docker is available
    try:
        import subprocess

        result = subprocess.run(
            ["docker", "--version"], capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            print("  ❌ Docker not available")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("  ❌ Docker not found or not responsive")
        print("  → Install Docker and ensure it's running")
        return False

    # Check docker-compose files
    compose_files = ["docker-compose.yml", "docker-compose-cloud.yml"]
    missing_files = [f for f in compose_files if not Path(f).exists()]

    if missing_files:
        print(f"  ⚠ Missing Docker Compose files: {missing_files}")
        return False

    # Check HOST_DATA_DIR if set
    doctor = SetupDoctor()
    env_vars = doctor.load_environment_variables()
    host_data_dir = env_vars.get("HOST_DATA_DIR", "")

    if host_data_dir and not Path(host_data_dir).exists():
        print(f"  ❌ HOST_DATA_DIR path doesn't exist: {host_data_dir}")
        print("  → Create the directory or update the path in .env")
        return False

    print("  ✓ Docker configuration ready")
    return True


def check_terraform_config():
    """Check Terraform configuration"""
    terraform_dir = Path("infra")

    if not terraform_dir.exists():
        print("  ❌ Terraform directory (infra/) not found")
        return False

    required_files = ["main.tf", "variables.tf", "outputs.tf"]
    missing_files = [f for f in required_files if not (terraform_dir / f).exists()]

    if missing_files:
        print(f"  ⚠ Missing Terraform files: {missing_files}")

    # Check if terraform is available
    try:
        import subprocess

        result = subprocess.run(
            ["terraform", "--version"], capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            print("  ⚠ Terraform not available")
            print("  → Install Terraform for infrastructure management")
            return True  # Not critical for basic setup
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("  ⚠ Terraform not found")
        print("  → Install Terraform for infrastructure management")
        return True  # Not critical for basic setup

    print("  ✓ Terraform configuration available")
    return True


def show_next_steps():
    """Show recommended next steps after setup validation"""
    print("\nRECOMMENDED NEXT STEPS")
    print("=" * 30)
    print("1. Build Docker images:")
    print("   docker-compose build")
    print()
    print("2. Provision infrastructure (if using AWS):")
    print("   source export_tf_vars.sh")
    print("   cd infra && terraform init && terraform apply")
    print()
    print("3. Start the pipeline:")
    print("   docker-compose up -d")
    print()
    print("4. Access Airflow UI:")
    print("   http://localhost:8080")
    print()
    print("5. Monitor pipeline:")
    print("   Check Airflow DAGs and logs")
    print("   Monitor S3 buckets for data flow")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Setup Doctor - Quick configuration validation for Sales Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_doctor.py                    # Quick health check
  python setup_doctor.py --full             # Full validation with connectivity
  python setup_doctor.py --prod             # Production environment validation
  python setup_doctor.py --check-setup      # Guided setup verification
        """,
    )

    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full validation including connectivity tests",
    )

    parser.add_argument(
        "--prod", action="store_true", help="Validate production environment"
    )

    parser.add_argument(
        "--staging", action="store_true", help="Validate staging environment"
    )

    parser.add_argument(
        "--check-setup", action="store_true", help="Run guided setup verification"
    )

    parser.add_argument(
        "--next-steps", action="store_true", help="Show recommended next steps"
    )

    args = parser.parse_args()

    # Determine what to run
    if args.check_setup:
        success = guided_setup_check()
        if success and args.next_steps:
            show_next_steps()
    elif args.full:
        env = "prod" if args.prod else "staging" if args.staging else "dev"
        success = full_validation(env)
    elif args.prod:
        success = full_validation("prod")
    elif args.staging:
        success = full_validation("staging")
    elif args.next_steps:
        show_next_steps()
        success = True
    else:
        success = quick_health_check()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
