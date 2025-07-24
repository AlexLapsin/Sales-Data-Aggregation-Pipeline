# Dockerfile — build the ETL pipeline image
FROM python:3.11-slim

# Ensure logs are unbuffered
ENV PYTHONUNBUFFERED=1

# Install OS-level deps for psycopg2
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      libpq-dev \
      postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy runtime requirements and install only runtime deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and placeholder .env
COPY src/ ./src
COPY .env.example .env

# Default working directory for execution
WORKDIR /app/src

# Run the full pipeline by default
ENTRYPOINT ["bash", "-lc", "python -m scripts.create_tables && python -m scripts.run_extract && python -m scripts.run_transform && python -m scripts.run_load"]
