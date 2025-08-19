# Dockerfile — sales-pipeline image used by DockerOperator tasks
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# OS deps for psycopg2 + optional psql client (handy for debugging)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      libpq-dev \
      postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# App root
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src/ /app/src

# Work under src by default (tasks call "python -m scripts.*")
WORKDIR /app/src

ENTRYPOINT []
CMD ["python", "-c", "print('sales-pipeline image ready')"]
