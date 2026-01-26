#!/bin/bash
set -e

# Create pipeline_metadata database for quality metrics and job metadata
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE pipeline_metadata;
    GRANT ALL PRIVILEGES ON DATABASE pipeline_metadata TO airflow;
EOSQL

echo "pipeline_metadata database created successfully"
