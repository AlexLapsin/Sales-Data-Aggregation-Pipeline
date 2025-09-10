#!/usr/bin/env python3
"""
Snowflake setup script for sales data pipeline

This script automates the initial setup of Snowflake database objects
including databases, warehouses, schemas, and tables.

Usage:
    python setup_snowflake.py --config snowflake_connection_config.json --environment development
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import snowflake.connector
from snowflake.connector import DictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SnowflakeSetup:
    """Manages Snowflake database setup and configuration"""

    def __init__(self, config: Dict, environment: str):
        self.config = config
        self.environment = environment
        self.connection = None

        # Get environment-specific configuration
        self.env_config = config["environments"][environment]
        self.conn_params = config.get("connection_parameters", {})

        # SQL file execution order
        self.sql_files = [
            "01_database_setup.sql",
            "02_raw_tables.sql",
            "03_dimensional_tables.sql",
            "04_fact_tables.sql",
        ]

    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            # Prepare connection parameters
            conn_params = {
                "account": self.env_config["account"],
                "user": self.env_config["user"],
                "password": os.environ.get("SNOWFLAKE_PASSWORD")
                or self.env_config["password"],
                "role": self.env_config.get("role", "SYSADMIN"),
                "warehouse": self.env_config.get("warehouse"),
                "database": self.env_config.get("database"),
                "schema": self.env_config.get("schema"),
                **self.conn_params,
            }

            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}

            logger.info(f"Connecting to Snowflake account: {conn_params['account']}")
            self.connection = snowflake.connector.connect(**conn_params)
            logger.info("Successfully connected to Snowflake")

        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Snowflake")

    def execute_sql_file(self, file_path: Path) -> List[Dict]:
        """Execute SQL commands from file"""
        logger.info(f"Executing SQL file: {file_path.name}")

        try:
            with open(file_path, "r") as file:
                sql_content = file.read()

            # Split SQL file into individual statements
            statements = [
                stmt.strip() for stmt in sql_content.split(";") if stmt.strip()
            ]

            results = []
            with self.connection.cursor(DictCursor) as cursor:
                for i, statement in enumerate(statements, 1):
                    try:
                        # Skip comments and empty statements
                        if statement.startswith("--") or not statement:
                            continue

                        logger.debug(f"Executing statement {i}: {statement[:100]}...")
                        cursor.execute(statement)

                        # Fetch results if available
                        if cursor.description:
                            result = cursor.fetchall()
                            results.extend(result)

                    except Exception as e:
                        logger.warning(f"Statement {i} failed: {e}")
                        # Continue with other statements
                        continue

            logger.info(f"Completed execution of {file_path.name}")
            return results

        except Exception as e:
            logger.error(f"Failed to execute SQL file {file_path}: {e}")
            raise

    def verify_setup(self) -> Dict:
        """Verify that setup completed successfully"""
        logger.info("Verifying Snowflake setup...")

        verification_queries = {
            "databases": "SHOW DATABASES LIKE 'SALES_DW%'",
            "warehouses": "SHOW WAREHOUSES LIKE '%_WH'",
            "schemas": "SHOW SCHEMAS IN DATABASE SALES_DW",
            "raw_tables": "SHOW TABLES IN SCHEMA SALES_DW.RAW",
            "mart_tables": "SHOW TABLES IN SCHEMA SALES_DW.MARTS",
        }

        results = {}

        try:
            with self.connection.cursor(DictCursor) as cursor:
                for name, query in verification_queries.items():
                    try:
                        cursor.execute(query)
                        result = cursor.fetchall()
                        results[name] = len(result)
                        logger.info(f"Found {len(result)} {name}")
                    except Exception as e:
                        logger.warning(f"Verification query failed for {name}: {e}")
                        results[name] = 0

            return results

        except Exception as e:
            logger.error(f"Setup verification failed: {e}")
            raise

    def run_setup(self, sql_directory: Path) -> None:
        """Execute complete Snowflake setup"""
        logger.info(f"Starting Snowflake setup for environment: {self.environment}")

        try:
            self.connect()

            # Execute SQL files in order
            for sql_file in self.sql_files:
                file_path = sql_directory / sql_file
                if file_path.exists():
                    self.execute_sql_file(file_path)
                else:
                    logger.warning(f"SQL file not found: {file_path}")

            # Verify setup
            verification_results = self.verify_setup()

            logger.info("Snowflake setup completed successfully")
            logger.info(f"Setup verification results: {verification_results}")

        except Exception as e:
            logger.error(f"Snowflake setup failed: {e}")
            raise

        finally:
            self.disconnect()


def load_config(config_path: str) -> Dict:
    """Load configuration from JSON file"""
    try:
        with open(config_path, "r") as file:
            config = json.load(file)

        # Expand environment variables in config
        config_str = json.dumps(config)
        for env_var in os.environ:
            config_str = config_str.replace(f"${{{env_var}}}", os.environ[env_var])

        return json.loads(config_str)

    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Snowflake Database Setup")
    parser.add_argument(
        "--config",
        type=str,
        default="snowflake_connection_config.json",
        help="Path to Snowflake configuration file",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="development",
        choices=["development", "staging", "production"],
        help="Environment to set up",
    )
    parser.add_argument(
        "--sql-dir", type=str, default=".", help="Directory containing SQL setup files"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Load configuration
        logger.info("Loading Snowflake configuration...")
        config = load_config(args.config)

        # Validate environment
        if args.environment not in config["environments"]:
            raise ValueError(
                f"Environment '{args.environment}' not found in configuration"
            )

        # Setup SQL directory path
        sql_directory = Path(args.sql_dir).resolve()
        logger.info(f"Using SQL directory: {sql_directory}")

        # Run setup
        setup = SnowflakeSetup(config, args.environment)
        setup.run_setup(sql_directory)

        logger.info("Snowflake setup process completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
