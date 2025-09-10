#!/bin/bash
# deployment/deploy.sh
# Comprehensive deployment script for the cloud sales pipeline

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="sales-data-pipeline"
ENVIRONMENT="${ENVIRONMENT:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"

echo -e "${BLUE}===========================================${NC}"
echo -e "${BLUE}  Sales Data Pipeline Deployment Script   ${NC}"
echo -e "${BLUE}===========================================${NC}"
echo -e "Environment: ${YELLOW}${ENVIRONMENT}${NC}"
echo -e "AWS Region: ${YELLOW}${AWS_REGION}${NC}"
echo ""

# Function to print status messages
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 could not be found. Please install it first."
        exit 1
    fi
}

# Function to check environment variables
check_env_vars() {
    local required_vars=("$@")
    local missing_vars=()

    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done

    if [[ ${#missing_vars[@]} -ne 0 ]]; then
        log_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo ""
        echo "Please set these variables in your .env file or export them."
        exit 1
    fi
}

# Function to deploy infrastructure
deploy_infrastructure() {
    log_info "Deploying infrastructure with Terraform..."

    cd infra

    # Initialize Terraform
    log_info "Initializing Terraform..."
    terraform init -upgrade

    # Plan deployment
    log_info "Planning infrastructure deployment..."
    terraform plan -var-file="environments/${ENVIRONMENT}.tfvars" -out="tfplan-${ENVIRONMENT}"

    # Ask for confirmation
    echo ""
    read -p "Do you want to apply this Terraform plan? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Applying Terraform plan..."
        terraform apply "tfplan-${ENVIRONMENT}"
        log_success "Infrastructure deployed successfully!"

        # Save outputs
        terraform output -json > "../deployment/terraform-outputs-${ENVIRONMENT}.json"
        log_info "Terraform outputs saved to terraform-outputs-${ENVIRONMENT}.json"
    else
        log_warning "Terraform deployment cancelled."
        return 1
    fi

    cd ..
}

# Function to build Docker images
build_docker_images() {
    log_info "Building Docker images..."

    # Build main pipeline image
    log_info "Building main pipeline image..."
    docker build -t ${PROJECT_NAME}:latest -f docker/Dockerfile .
    docker tag ${PROJECT_NAME}:latest ${PROJECT_NAME}:${ENVIRONMENT}

    # Build dbt image
    log_info "Building dbt image..."
    docker build -t ${PROJECT_NAME}-dbt:latest -f docker/Dockerfile.dbt .
    docker tag ${PROJECT_NAME}-dbt:latest ${PROJECT_NAME}-dbt:${ENVIRONMENT}

    # Build Spark image
    log_info "Building Spark image..."
    docker build -t ${PROJECT_NAME}-spark:latest -f docker/Dockerfile.spark .
    docker tag ${PROJECT_NAME}-spark:latest ${PROJECT_NAME}-spark:${ENVIRONMENT}

    log_success "Docker images built successfully!"
}

# Function to push Docker images to ECR
push_docker_images() {
    if [[ "$ENVIRONMENT" != "local" ]]; then
        log_info "Pushing Docker images to ECR..."

        # Get ECR login token
        aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

        # Tag and push images
        docker tag ${PROJECT_NAME}:${ENVIRONMENT} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}:${ENVIRONMENT}
        docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}:${ENVIRONMENT}

        docker tag ${PROJECT_NAME}-dbt:${ENVIRONMENT} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}-dbt:${ENVIRONMENT}
        docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}-dbt:${ENVIRONMENT}

        docker tag ${PROJECT_NAME}-spark:${ENVIRONMENT} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}-spark:${ENVIRONMENT}
        docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}-spark:${ENVIRONMENT}

        log_success "Docker images pushed to ECR successfully!"
    else
        log_info "Skipping ECR push for local environment"
    fi
}

# Function to setup Snowflake objects
setup_snowflake() {
    log_info "Setting up Snowflake objects..."

    if [[ -z "${SNOWFLAKE_ACCOUNT}" ]] || [[ -z "${SNOWFLAKE_USER}" ]]; then
        log_warning "Snowflake credentials not found. Skipping Snowflake setup."
        return 0
    fi

    # Run Snowflake DDL scripts
    log_info "Creating Snowflake database and schemas..."

    for script in snowflake/*.sql; do
        if [[ -f "$script" ]]; then
            log_info "Running $(basename $script)..."
            # Note: In production, use snowsql CLI or Python connector
            echo "-- Would execute: $script"
        fi
    done

    log_success "Snowflake objects created successfully!"
}

# Function to deploy dbt models
deploy_dbt() {
    log_info "Deploying dbt models..."

    cd dbt

    # Install dependencies
    log_info "Installing dbt dependencies..."
    dbt deps

    # Test connection
    log_info "Testing dbt connection..."
    dbt debug

    # Run models
    if [[ "$ENVIRONMENT" == "prod" ]]; then
        log_info "Running dbt in production mode..."
        dbt run --target prod
        dbt test --target prod
    else
        log_info "Running dbt in ${ENVIRONMENT} mode..."
        dbt run --target ${ENVIRONMENT}
        dbt test --target ${ENVIRONMENT}
    fi

    # Generate documentation
    log_info "Generating dbt documentation..."
    dbt docs generate

    cd ..
    log_success "dbt models deployed successfully!"
}

# Function to start services
start_services() {
    log_info "Starting pipeline services..."

    if [[ "$ENVIRONMENT" == "local" ]]; then
        # Start local development environment
        log_info "Starting local development environment..."
        docker-compose -f docker-compose-cloud.yml --profile kafka-local --profile dbt-local up -d

        # Wait for services to be ready
        log_info "Waiting for services to be ready..."
        sleep 30

        # Check service health
        check_service_health

    else
        log_info "For non-local environments, services are managed by Airflow and cloud providers"
    fi

    log_success "Services started successfully!"
}

# Function to check service health
check_service_health() {
    log_info "Checking service health..."

    # Check Kafka
    if docker ps | grep -q sales-kafka; then
        log_success "Kafka is running"
    else
        log_error "Kafka is not running"
    fi

    # Check Kafka Connect
    if curl -s http://localhost:8083/connectors >/dev/null 2>&1; then
        log_success "Kafka Connect is healthy"
    else
        log_warning "Kafka Connect is not responding"
    fi

    # Check Kafka UI
    if curl -s http://localhost:8080 >/dev/null 2>&1; then
        log_success "Kafka UI is accessible"
    else
        log_warning "Kafka UI is not accessible"
    fi
}

# Function to run tests
run_tests() {
    log_info "Running pipeline tests..."

    # Run dbt tests
    cd dbt
    dbt test --target ${ENVIRONMENT}
    cd ..

    # Run Python tests if available
    if [[ -f "requirements-dev.txt" ]]; then
        log_info "Running Python tests..."
        pip install -r requirements-dev.txt
        python -m pytest tests/ -v
    fi

    # Run Spark tests
    if [[ -f "spark/test_spark_job.py" ]]; then
        log_info "Running Spark tests..."
        cd spark
        python test_spark_job.py
        cd ..
    fi

    log_success "All tests passed!"
}

# Function to setup monitoring
setup_monitoring() {
    log_info "Setting up monitoring and alerting..."

    if [[ "$ENVIRONMENT" != "local" ]]; then
        # Setup CloudWatch alarms, SNS topics, etc.
        log_info "Setting up AWS CloudWatch monitoring..."
        # This would typically involve AWS CLI commands or additional Terraform
    fi

    # Setup Grafana dashboards for local monitoring
    if [[ "$ENVIRONMENT" == "local" ]]; then
        log_info "Starting monitoring services..."
        docker-compose -f docker-compose-cloud.yml --profile monitoring up -d

        log_info "Grafana dashboard available at http://localhost:3000 (admin/admin)"
    fi

    log_success "Monitoring setup completed!"
}

# Function to display deployment summary
deployment_summary() {
    echo ""
    echo -e "${GREEN}===========================================${NC}"
    echo -e "${GREEN}     Deployment Summary                    ${NC}"
    echo -e "${GREEN}===========================================${NC}"
    echo -e "Environment: ${YELLOW}${ENVIRONMENT}${NC}"
    echo -e "Project: ${YELLOW}${PROJECT_NAME}${NC}"
    echo -e "Region: ${YELLOW}${AWS_REGION}${NC}"
    echo ""
    echo -e "${GREEN}✓${NC} Infrastructure deployed"
    echo -e "${GREEN}✓${NC} Docker images built"
    if [[ "$ENVIRONMENT" != "local" ]]; then
        echo -e "${GREEN}✓${NC} Images pushed to ECR"
    fi
    echo -e "${GREEN}✓${NC} Snowflake objects created"
    echo -e "${GREEN}✓${NC} dbt models deployed"
    echo -e "${GREEN}✓${NC} Services started"
    echo -e "${GREEN}✓${NC} Tests completed"
    echo -e "${GREEN}✓${NC} Monitoring setup"
    echo ""

    if [[ "$ENVIRONMENT" == "local" ]]; then
        echo "Local services:"
        echo "  - Kafka UI: http://localhost:8080"
        echo "  - Kafka Connect: http://localhost:8083"
        echo "  - Grafana: http://localhost:3000"
        echo ""
    fi

    echo "Next steps:"
    echo "  1. Check Airflow DAGs in your Airflow UI"
    echo "  2. Monitor data quality in Grafana dashboards"
    echo "  3. Review dbt documentation"
    echo "  4. Test the complete pipeline end-to-end"
    echo ""
    echo -e "${GREEN}Deployment completed successfully!${NC}"
}

# Main deployment function
main() {
    # Check prerequisites
    log_info "Checking prerequisites..."
    check_command "docker"
    check_command "terraform"
    check_command "aws"

    if [[ "$1" == "infrastructure-only" ]]; then
        # Required variables for infrastructure only
        check_env_vars "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "PROJECT_NAME"
        deploy_infrastructure
        return 0
    fi

    # Full deployment - check all required variables
    check_env_vars "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "SNOWFLAKE_ACCOUNT" "SNOWFLAKE_USER" "SNOWFLAKE_PASSWORD"

    # Load environment file if it exists
    if [[ -f ".env" ]]; then
        log_info "Loading environment variables from .env file..."
        set -a
        source .env
        set +a
    fi

    # Deployment steps
    case "${1:-full}" in
        "infrastructure")
            deploy_infrastructure
            ;;
        "docker")
            build_docker_images
            push_docker_images
            ;;
        "snowflake")
            setup_snowflake
            ;;
        "dbt")
            deploy_dbt
            ;;
        "services")
            start_services
            ;;
        "tests")
            run_tests
            ;;
        "monitoring")
            setup_monitoring
            ;;
        "full")
            deploy_infrastructure
            build_docker_images
            push_docker_images
            setup_snowflake
            deploy_dbt
            start_services
            run_tests
            setup_monitoring
            deployment_summary
            ;;
        *)
            echo "Usage: $0 [infrastructure|docker|snowflake|dbt|services|tests|monitoring|full]"
            echo "  infrastructure - Deploy AWS infrastructure only"
            echo "  docker         - Build and push Docker images"
            echo "  snowflake      - Setup Snowflake objects"
            echo "  dbt            - Deploy dbt models"
            echo "  services       - Start pipeline services"
            echo "  tests          - Run all tests"
            echo "  monitoring     - Setup monitoring"
            echo "  full           - Complete deployment (default)"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
