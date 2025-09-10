-- snowflake/01_database_setup.sql
-- Initial database and warehouse setup for sales data pipeline
-- Run this script first to establish the foundation

-- Use ACCOUNTADMIN role for initial setup
USE ROLE ACCOUNTADMIN;

-- Create warehouse for ETL operations
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
         AUTO_SUSPEND = 300
         AUTO_RESUME = TRUE
         MIN_CLUSTER_COUNT = 1
         MAX_CLUSTER_COUNT = 1
         SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for ETL operations including Kafka ingestion';

-- Create warehouse for analytics/dbt operations
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
         AUTO_SUSPEND = 300
         AUTO_RESUME = TRUE
         MIN_CLUSTER_COUNT = 1
         MAX_CLUSTER_COUNT = 1
         SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for analytics queries and dbt transformations';

-- Create database for sales data warehouse
CREATE DATABASE IF NOT EXISTS SALES_DW
    COMMENT = 'Sales data warehouse for streaming and batch analytics';

-- Use the database and set context
USE DATABASE SALES_DW;
USE WAREHOUSE ETL_WH;

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data ingested from Kafka and batch sources';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging area for data transformations and cleansing';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Analytics-ready data marts and dimensional models';

-- Create schema for utilities and metadata
CREATE SCHEMA IF NOT EXISTS UTILS
    COMMENT = 'Utility tables, logging, and pipeline metadata';

-- Grant usage on database and warehouses to PUBLIC role
GRANT USAGE ON DATABASE SALES_DW TO ROLE PUBLIC;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE PUBLIC;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PUBLIC;

-- Grant schema permissions
GRANT USAGE ON SCHEMA SALES_DW.RAW TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA SALES_DW.STAGING TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA SALES_DW.MARTS TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA SALES_DW.UTILS TO ROLE PUBLIC;

-- Display setup confirmation
SELECT 'Database and warehouse setup completed successfully' AS status;
