# Dockerfile — sales-pipeline image used by DockerOperator tasks
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# OS deps for various database clients and processing tools
# PostgreSQL: for legacy support (psycopg2)
# ODBC: for Snowflake/Databricks connectivity
# Build tools: for various Python packages
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      g++ \
      libpq-dev \
      postgresql-client \
      unixodbc-dev \
      curl \
      gnupg \
 && rm -rf /var/lib/apt/lists/*

# App root
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src/ /app/src

# Copy legacy components (for backward compatibility)
COPY legacy/ /app/legacy

# Work under app root for proper Python path resolution
WORKDIR /app
ENV PYTHONPATH=/app

ENTRYPOINT []
CMD ["python", "-c", "print('sales-pipeline image ready - supports both modern and legacy components')"]
