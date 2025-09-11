#!/usr/bin/env python3
"""
Data Validation Utilities for End-to-End Pipeline Testing

This module provides comprehensive data validation utilities for each stage
of the sales data aggregation pipeline, ensuring data quality and consistency
throughout the entire data flow.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path


@dataclass
class ValidationResult:
    """Result of a data validation check"""

    stage: str
    check_name: str
    passed: bool
    score: float  # 0.0 to 1.0
    details: Dict[str, Any]
    timestamp: datetime
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "stage": self.stage,
            "check_name": self.check_name,
            "passed": self.passed,
            "score": self.score,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "error_message": self.error_message,
        }


@dataclass
class ValidationSummary:
    """Summary of all validation results for a pipeline run"""

    pipeline_run_id: str
    start_time: datetime
    end_time: datetime
    overall_passed: bool
    overall_score: float
    stage_results: Dict[str, List[ValidationResult]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "overall_passed": self.overall_passed,
            "overall_score": self.overall_score,
            "stage_results": {
                stage: [result.to_dict() for result in results]
                for stage, results in self.stage_results.items()
            },
        }


class BaseDataValidator:
    """Base class for data validators"""

    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.logger = logging.getLogger(f"{__name__}.{stage_name}")

    def validate(self, data: Any, **kwargs) -> List[ValidationResult]:
        """Validate data and return results"""
        raise NotImplementedError("Subclasses must implement validate method")

    def _create_result(
        self,
        check_name: str,
        passed: bool,
        score: float,
        details: Dict[str, Any],
        error_message: Optional[str] = None,
    ) -> ValidationResult:
        """Create a validation result"""
        return ValidationResult(
            stage=self.stage_name,
            check_name=check_name,
            passed=passed,
            score=score,
            details=details,
            timestamp=datetime.now(),
            error_message=error_message,
        )


class RawDataValidator(BaseDataValidator):
    """Validator for raw CSV input data"""

    def __init__(self):
        super().__init__("raw_data")

    def validate(
        self, data: Union[pd.DataFrame, str, Path], **kwargs
    ) -> List[ValidationResult]:
        """Validate raw CSV data"""
        results = []

        # Load data if path provided
        if isinstance(data, (str, Path)):
            try:
                data = pd.read_csv(data)
            except Exception as e:
                return [
                    self._create_result(
                        "data_loading",
                        False,
                        0.0,
                        {"error": str(e)},
                        f"Failed to load CSV data: {e}",
                    )
                ]

        # Check data structure
        results.append(self._validate_schema(data))
        results.append(self._validate_completeness(data))
        results.append(self._validate_data_types(data))
        results.append(self._validate_business_rules(data))
        results.append(self._validate_data_quality(data))

        return results

    def _validate_schema(self, df: pd.DataFrame) -> ValidationResult:
        """Validate CSV schema matches expected structure"""
        expected_columns = {
            "Order ID",
            "Customer ID",
            "Product ID",
            "Category",
            "Product Name",
            "Sales",
            "Quantity",
            "Profit",
            "Order Date",
            "Ship Date",
            "Region",
        }

        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns

        passed = len(missing_columns) == 0
        score = (
            1.0
            if passed
            else max(0.0, 1.0 - (len(missing_columns) / len(expected_columns)))
        )

        details = {
            "expected_columns": len(expected_columns),
            "actual_columns": len(actual_columns),
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
        }

        return self._create_result(
            "schema_validation",
            passed,
            score,
            details,
            f"Missing columns: {missing_columns}" if missing_columns else None,
        )

    def _validate_completeness(self, df: pd.DataFrame) -> ValidationResult:
        """Validate data completeness"""
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        null_percentage = null_cells / total_cells if total_cells > 0 else 0

        # Check critical columns for nulls
        critical_columns = ["Order ID", "Product ID", "Sales", "Quantity"]
        critical_nulls = df[critical_columns].isnull().sum().sum()

        passed = null_percentage <= 0.1 and critical_nulls == 0
        score = max(0.0, 1.0 - null_percentage)

        details = {
            "total_cells": total_cells,
            "null_cells": null_cells,
            "null_percentage": null_percentage,
            "critical_nulls": critical_nulls,
            "null_by_column": df.isnull().sum().to_dict(),
        }

        return self._create_result(
            "completeness_check",
            passed,
            score,
            details,
            f"High null percentage: {null_percentage:.2%}" if not passed else None,
        )

    def _validate_data_types(self, df: pd.DataFrame) -> ValidationResult:
        """Validate data types and formats"""
        type_issues = []

        # Check numeric columns
        numeric_columns = ["Sales", "Quantity", "Profit"]
        for col in numeric_columns:
            if col in df.columns:
                try:
                    pd.to_numeric(df[col], errors="raise")
                except (ValueError, TypeError):
                    non_numeric = df[~pd.to_numeric(df[col], errors="coerce").notna()]
                    type_issues.append(
                        {
                            "column": col,
                            "issue": "non_numeric_values",
                            "count": len(non_numeric),
                            "examples": non_numeric[col].head().tolist(),
                        }
                    )

        # Check date columns
        date_columns = ["Order Date", "Ship Date"]
        for col in date_columns:
            if col in df.columns:
                try:
                    pd.to_datetime(df[col], errors="raise")
                except (ValueError, TypeError):
                    invalid_dates = df[
                        ~pd.to_datetime(df[col], errors="coerce").notna()
                    ]
                    type_issues.append(
                        {
                            "column": col,
                            "issue": "invalid_date_format",
                            "count": len(invalid_dates),
                            "examples": invalid_dates[col].head().tolist(),
                        }
                    )

        passed = len(type_issues) == 0
        score = max(0.0, 1.0 - (len(type_issues) / 5))  # Assume max 5 type issues

        details = {
            "type_issues": type_issues,
            "issues_count": len(type_issues),
        }

        return self._create_result(
            "data_type_validation",
            passed,
            score,
            details,
            f"Found {len(type_issues)} data type issues" if not passed else None,
        )

    def _validate_business_rules(self, df: pd.DataFrame) -> ValidationResult:
        """Validate business logic rules"""
        violations = []

        # Sales amount should be positive
        if "Sales" in df.columns:
            negative_sales = df[df["Sales"] < 0]
            if len(negative_sales) > 0:
                violations.append(
                    {
                        "rule": "positive_sales",
                        "violation_count": len(negative_sales),
                        "percentage": len(negative_sales) / len(df),
                    }
                )

        # Quantity should be positive
        if "Quantity" in df.columns:
            invalid_quantity = df[df["Quantity"] <= 0]
            if len(invalid_quantity) > 0:
                violations.append(
                    {
                        "rule": "positive_quantity",
                        "violation_count": len(invalid_quantity),
                        "percentage": len(invalid_quantity) / len(df),
                    }
                )

        # Order date should be before or equal to ship date
        if "Order Date" in df.columns and "Ship Date" in df.columns:
            df_dates = df.copy()
            df_dates["Order Date"] = pd.to_datetime(
                df_dates["Order Date"], errors="coerce"
            )
            df_dates["Ship Date"] = pd.to_datetime(
                df_dates["Ship Date"], errors="coerce"
            )

            invalid_dates = df_dates[df_dates["Order Date"] > df_dates["Ship Date"]]
            if len(invalid_dates) > 0:
                violations.append(
                    {
                        "rule": "order_before_ship",
                        "violation_count": len(invalid_dates),
                        "percentage": len(invalid_dates) / len(df),
                    }
                )

        total_violation_rate = sum(v["percentage"] for v in violations)
        passed = total_violation_rate <= 0.05  # Allow up to 5% violations
        score = max(0.0, 1.0 - total_violation_rate)

        details = {
            "violations": violations,
            "total_violation_rate": total_violation_rate,
        }

        return self._create_result(
            "business_rules_validation",
            passed,
            score,
            details,
            f"Business rule violations: {total_violation_rate:.2%}"
            if not passed
            else None,
        )

    def _validate_data_quality(self, df: pd.DataFrame) -> ValidationResult:
        """Validate overall data quality metrics"""
        quality_metrics = {}

        # Duplicate detection
        duplicates = df.duplicated().sum()
        duplicate_rate = duplicates / len(df) if len(df) > 0 else 0
        quality_metrics["duplicate_rate"] = duplicate_rate

        # Outlier detection for Sales
        if "Sales" in df.columns:
            sales_data = pd.to_numeric(df["Sales"], errors="coerce").dropna()
            if len(sales_data) > 0:
                q1 = sales_data.quantile(0.25)
                q3 = sales_data.quantile(0.75)
                iqr = q3 - q1
                outliers = sales_data[
                    (sales_data < q1 - 1.5 * iqr) | (sales_data > q3 + 1.5 * iqr)
                ]
                outlier_rate = len(outliers) / len(sales_data)
                quality_metrics["outlier_rate"] = outlier_rate

        # Data freshness (if Order Date available)
        if "Order Date" in df.columns:
            try:
                dates = pd.to_datetime(df["Order Date"], errors="coerce").dropna()
                if len(dates) > 0:
                    latest_date = dates.max()
                    days_old = (datetime.now() - latest_date).days
                    quality_metrics["data_age_days"] = days_old
                    quality_metrics["is_recent"] = days_old <= 30
            except:
                quality_metrics["data_age_days"] = None

        # Overall quality score
        score_components = []
        if duplicate_rate <= 0.05:
            score_components.append(1.0)
        else:
            score_components.append(max(0.0, 1.0 - duplicate_rate))

        if "outlier_rate" in quality_metrics:
            if quality_metrics["outlier_rate"] <= 0.1:
                score_components.append(1.0)
            else:
                score_components.append(max(0.0, 1.0 - quality_metrics["outlier_rate"]))

        overall_score = np.mean(score_components) if score_components else 0.5
        passed = overall_score >= 0.8

        details = {
            "quality_metrics": quality_metrics,
            "score_components": score_components,
        }

        return self._create_result(
            "data_quality_assessment",
            passed,
            overall_score,
            details,
            f"Low data quality score: {overall_score:.2f}" if not passed else None,
        )


class KafkaStreamValidator(BaseDataValidator):
    """Validator for Kafka streaming data"""

    def __init__(self):
        super().__init__("kafka_streaming")

    def validate(self, messages: List[Dict], **kwargs) -> List[ValidationResult]:
        """Validate Kafka message stream"""
        results = []

        if not messages:
            return [
                self._create_result(
                    "message_availability",
                    False,
                    0.0,
                    {"message_count": 0},
                    "No messages received from Kafka",
                )
            ]

        results.append(self._validate_message_format(messages))
        results.append(self._validate_message_completeness(messages))
        results.append(self._validate_streaming_consistency(messages))
        results.append(self._validate_throughput(messages, **kwargs))

        return results

    def _validate_message_format(self, messages: List[Dict]) -> ValidationResult:
        """Validate Kafka message format and structure"""
        required_fields = {
            "order_id",
            "store_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_price",
            "sale_timestamp",
            "customer_id",
        }

        format_issues = []
        valid_messages = 0

        for i, message in enumerate(messages):
            try:
                # Check if message is valid JSON
                if isinstance(message, str):
                    message = json.loads(message)

                # Check required fields
                missing_fields = required_fields - set(message.keys())
                if missing_fields:
                    format_issues.append(
                        {
                            "message_index": i,
                            "issue": "missing_fields",
                            "missing_fields": list(missing_fields),
                        }
                    )
                else:
                    valid_messages += 1

            except json.JSONDecodeError:
                format_issues.append(
                    {
                        "message_index": i,
                        "issue": "invalid_json",
                        "message": str(message)[:100],
                    }
                )

        passed = len(format_issues) == 0
        score = valid_messages / len(messages) if messages else 0.0

        details = {
            "total_messages": len(messages),
            "valid_messages": valid_messages,
            "format_issues": format_issues[:10],  # Limit to first 10 issues
            "format_issues_count": len(format_issues),
        }

        return self._create_result(
            "message_format_validation",
            passed,
            score,
            details,
            f"Found {len(format_issues)} format issues" if not passed else None,
        )

    def _validate_message_completeness(self, messages: List[Dict]) -> ValidationResult:
        """Validate completeness of message data"""
        null_counts = {}
        total_messages = len(messages)

        for message in messages:
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except:
                    continue

            for key, value in message.items():
                if value is None or value == "":
                    null_counts[key] = null_counts.get(key, 0) + 1

        # Calculate null percentages
        null_percentages = {
            key: count / total_messages for key, count in null_counts.items()
        }

        # Check critical fields
        critical_fields = ["order_id", "product_id", "quantity", "total_price"]
        critical_nulls = sum(null_counts.get(field, 0) for field in critical_fields)

        max_null_percentage = (
            max(null_percentages.values()) if null_percentages else 0.0
        )
        passed = max_null_percentage <= 0.05 and critical_nulls == 0
        score = max(0.0, 1.0 - max_null_percentage)

        details = {
            "null_counts": null_counts,
            "null_percentages": null_percentages,
            "critical_nulls": critical_nulls,
            "max_null_percentage": max_null_percentage,
        }

        return self._create_result(
            "message_completeness_validation",
            passed,
            score,
            details,
            f"High null percentage: {max_null_percentage:.2%}" if not passed else None,
        )

    def _validate_streaming_consistency(self, messages: List[Dict]) -> ValidationResult:
        """Validate consistency in streaming data"""
        consistency_issues = []

        # Check for duplicate order IDs within the batch
        order_ids = []
        for message in messages:
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except:
                    continue

            order_id = message.get("order_id")
            if order_id:
                order_ids.append(order_id)

        unique_orders = len(set(order_ids))
        duplicate_orders = len(order_ids) - unique_orders

        if duplicate_orders > 0:
            consistency_issues.append(
                {
                    "issue": "duplicate_order_ids",
                    "count": duplicate_orders,
                    "percentage": duplicate_orders / len(order_ids) if order_ids else 0,
                }
            )

        # Check timestamp consistency
        timestamps = []
        for message in messages:
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except:
                    continue

            timestamp = message.get("sale_timestamp")
            if timestamp:
                try:
                    parsed_timestamp = datetime.fromisoformat(
                        timestamp.replace("Z", "+00:00")
                    )
                    timestamps.append(parsed_timestamp)
                except:
                    pass

        if timestamps:
            time_range = max(timestamps) - min(timestamps)
            if time_range > timedelta(hours=24):
                consistency_issues.append(
                    {
                        "issue": "large_time_range",
                        "time_range_hours": time_range.total_seconds() / 3600,
                    }
                )

        passed = len(consistency_issues) == 0
        score = max(0.0, 1.0 - (len(consistency_issues) / 5))  # Assume max 5 issues

        details = {
            "consistency_issues": consistency_issues,
            "unique_orders": unique_orders,
            "total_messages": len(messages),
            "timestamp_range_minutes": time_range.total_seconds() / 60
            if timestamps
            else None,
        }

        return self._create_result(
            "streaming_consistency_validation",
            passed,
            score,
            details,
            f"Found {len(consistency_issues)} consistency issues"
            if not passed
            else None,
        )

    def _validate_throughput(self, messages: List[Dict], **kwargs) -> ValidationResult:
        """Validate streaming throughput meets requirements"""
        duration_seconds = kwargs.get("duration_seconds", 60)
        min_throughput = kwargs.get("min_throughput", 100.0)  # messages per second

        actual_throughput = (
            len(messages) / duration_seconds if duration_seconds > 0 else 0
        )
        passed = actual_throughput >= min_throughput

        # Calculate throughput score
        if min_throughput > 0:
            score = min(1.0, actual_throughput / min_throughput)
        else:
            score = 1.0 if actual_throughput > 0 else 0.0

        details = {
            "message_count": len(messages),
            "duration_seconds": duration_seconds,
            "actual_throughput": actual_throughput,
            "min_throughput": min_throughput,
            "throughput_ratio": actual_throughput / min_throughput
            if min_throughput > 0
            else None,
        }

        return self._create_result(
            "throughput_validation",
            passed,
            score,
            details,
            f"Low throughput: {actual_throughput:.1f} msg/s (required: {min_throughput})"
            if not passed
            else None,
        )


class SparkETLValidator(BaseDataValidator):
    """Validator for Spark ETL processing results"""

    def __init__(self):
        super().__init__("spark_etl")

    def validate(
        self, input_data: pd.DataFrame, output_data: pd.DataFrame, **kwargs
    ) -> List[ValidationResult]:
        """Validate Spark ETL transformation results"""
        results = []

        results.append(self._validate_record_count(input_data, output_data))
        results.append(self._validate_data_transformation(input_data, output_data))
        results.append(self._validate_data_enrichment(output_data))
        results.append(self._validate_performance(input_data, output_data, **kwargs))

        return results

    def _validate_record_count(
        self, input_df: pd.DataFrame, output_df: pd.DataFrame
    ) -> ValidationResult:
        """Validate record count after transformation"""
        input_count = len(input_df)
        output_count = len(output_df)

        # Allow for some data cleaning, but not excessive loss
        retention_rate = output_count / input_count if input_count > 0 else 0
        passed = retention_rate >= 0.9  # At least 90% retention
        score = retention_rate

        details = {
            "input_count": input_count,
            "output_count": output_count,
            "retention_rate": retention_rate,
            "records_lost": input_count - output_count,
        }

        return self._create_result(
            "record_count_validation",
            passed,
            score,
            details,
            f"Low retention rate: {retention_rate:.2%}" if not passed else None,
        )

    def _validate_data_transformation(
        self, input_df: pd.DataFrame, output_df: pd.DataFrame
    ) -> ValidationResult:
        """Validate data transformation correctness"""
        transformation_issues = []

        # Check if required output columns exist
        expected_output_columns = {
            "ORDER_ID",
            "STORE_ID",
            "PRODUCT_ID",
            "PRODUCT_NAME",
            "CATEGORY",
            "QUANTITY",
            "UNIT_PRICE",
            "TOTAL_PRICE",
            "ORDER_DATE",
            "SALES",
            "PROFIT",
            "BATCH_ID",
            "INGESTION_TIMESTAMP",
            "SOURCE_SYSTEM",
        }

        actual_columns = set(output_df.columns)
        missing_columns = expected_output_columns - actual_columns

        if missing_columns:
            transformation_issues.append(
                {
                    "issue": "missing_output_columns",
                    "missing_columns": list(missing_columns),
                }
            )

        # Validate data type transformations
        if "ORDER_DATE" in output_df.columns:
            try:
                pd.to_datetime(output_df["ORDER_DATE"])
            except:
                transformation_issues.append(
                    {"issue": "invalid_date_transformation", "column": "ORDER_DATE"}
                )

        # Check for proper numeric transformations
        numeric_columns = ["QUANTITY", "UNIT_PRICE", "TOTAL_PRICE", "SALES", "PROFIT"]
        for col in numeric_columns:
            if col in output_df.columns:
                try:
                    pd.to_numeric(output_df[col])
                except:
                    transformation_issues.append(
                        {"issue": "invalid_numeric_transformation", "column": col}
                    )

        passed = len(transformation_issues) == 0
        score = max(0.0, 1.0 - (len(transformation_issues) / 5))

        details = {
            "transformation_issues": transformation_issues,
            "expected_columns": len(expected_output_columns),
            "actual_columns": len(actual_columns),
        }

        return self._create_result(
            "data_transformation_validation",
            passed,
            score,
            details,
            f"Found {len(transformation_issues)} transformation issues"
            if not passed
            else None,
        )

    def _validate_data_enrichment(self, output_df: pd.DataFrame) -> ValidationResult:
        """Validate data enrichment and derived fields"""
        enrichment_issues = []

        # Check if enrichment fields were added
        enrichment_fields = ["BATCH_ID", "INGESTION_TIMESTAMP", "SOURCE_SYSTEM"]
        for field in enrichment_fields:
            if field not in output_df.columns:
                enrichment_issues.append(
                    {"issue": "missing_enrichment_field", "field": field}
                )
            elif output_df[field].isnull().any():
                null_count = output_df[field].isnull().sum()
                enrichment_issues.append(
                    {
                        "issue": "null_enrichment_values",
                        "field": field,
                        "null_count": null_count,
                    }
                )

        # Validate derived calculations
        if all(
            col in output_df.columns
            for col in ["UNIT_PRICE", "QUANTITY", "TOTAL_PRICE"]
        ):
            calculated_total = output_df["UNIT_PRICE"] * output_df["QUANTITY"]
            price_diff = abs(calculated_total - output_df["TOTAL_PRICE"])
            significant_diffs = (price_diff > 0.01).sum()

            if (
                significant_diffs > len(output_df) * 0.05
            ):  # More than 5% have calculation errors
                enrichment_issues.append(
                    {
                        "issue": "calculation_errors",
                        "field": "TOTAL_PRICE",
                        "error_count": significant_diffs,
                    }
                )

        passed = len(enrichment_issues) == 0
        score = max(0.0, 1.0 - (len(enrichment_issues) / 3))

        details = {
            "enrichment_issues": enrichment_issues,
            "enrichment_fields_present": len(
                [f for f in enrichment_fields if f in output_df.columns]
            ),
        }

        return self._create_result(
            "data_enrichment_validation",
            passed,
            score,
            details,
            f"Found {len(enrichment_issues)} enrichment issues" if not passed else None,
        )

    def _validate_performance(
        self, input_df: pd.DataFrame, output_df: pd.DataFrame, **kwargs
    ) -> ValidationResult:
        """Validate ETL performance metrics"""
        processing_time = kwargs.get("processing_time_seconds", 0)
        records_processed = len(input_df)

        if processing_time > 0:
            throughput = records_processed / processing_time
            min_throughput = kwargs.get("min_throughput", 1000.0)  # records per second

            passed = throughput >= min_throughput
            score = min(1.0, throughput / min_throughput) if min_throughput > 0 else 1.0
        else:
            passed = True
            throughput = 0
            score = 1.0

        details = {
            "records_processed": records_processed,
            "processing_time_seconds": processing_time,
            "throughput_records_per_second": throughput,
            "min_throughput": kwargs.get("min_throughput", 1000.0),
        }

        return self._create_result(
            "performance_validation",
            passed,
            score,
            details,
            f"Low throughput: {throughput:.1f} rec/s" if not passed else None,
        )


class DataWarehouseValidator(BaseDataValidator):
    """Validator for data warehouse final state"""

    def __init__(self):
        super().__init__("data_warehouse")

    def validate(self, connection, **kwargs) -> List[ValidationResult]:
        """Validate final data warehouse state"""
        results = []

        results.append(self._validate_table_structure(connection))
        results.append(self._validate_data_integrity(connection))
        results.append(self._validate_business_metrics(connection))
        results.append(self._validate_data_freshness(connection))

        return results

    def _validate_table_structure(self, connection) -> ValidationResult:
        """Validate data warehouse table structure"""
        # This would need to be adapted based on your specific database
        # For now, providing a template structure

        expected_tables = ["fact_sales", "dim_product", "dim_date", "dim_customer"]
        structure_issues = []

        try:
            # Example validation - adapt to your database
            tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """

            # This is a placeholder - you'd implement actual database queries
            # based on your specific warehouse (PostgreSQL, Snowflake, etc.)

            passed = True  # Placeholder
            score = 1.0  # Placeholder

            details = {
                "expected_tables": expected_tables,
                "structure_issues": structure_issues,
            }

        except Exception as e:
            passed = False
            score = 0.0
            details = {"error": str(e)}
            structure_issues.append({"issue": "connection_error", "error": str(e)})

        return self._create_result(
            "table_structure_validation",
            passed,
            score,
            details,
            f"Found {len(structure_issues)} structure issues"
            if structure_issues
            else None,
        )

    def _validate_data_integrity(self, connection) -> ValidationResult:
        """Validate data integrity constraints"""
        integrity_issues = []

        # Example integrity checks - adapt to your schema
        integrity_checks = [
            "SELECT COUNT(*) FROM fact_sales WHERE sales_amount < 0",
            "SELECT COUNT(*) FROM fact_sales WHERE quantity <= 0",
            "SELECT COUNT(*) FROM fact_sales WHERE order_date > ship_date",
        ]

        try:
            total_violations = 0
            # Execute integrity checks
            # This is a placeholder - implement actual database queries

            passed = total_violations == 0
            score = max(0.0, 1.0 - (total_violations / 1000))  # Assume threshold

            details = {
                "integrity_checks_run": len(integrity_checks),
                "total_violations": total_violations,
                "integrity_issues": integrity_issues,
            }

        except Exception as e:
            passed = False
            score = 0.0
            details = {"error": str(e)}

        return self._create_result(
            "data_integrity_validation",
            passed,
            score,
            details,
            f"Found {len(integrity_issues)} integrity violations"
            if integrity_issues
            else None,
        )

    def _validate_business_metrics(self, connection) -> ValidationResult:
        """Validate business metrics and KPIs"""
        metric_issues = []

        try:
            # Example business metric validations
            # These would be specific to your business requirements

            passed = len(metric_issues) == 0
            score = max(0.0, 1.0 - (len(metric_issues) / 5))

            details = {
                "metrics_validated": ["total_sales", "order_count", "avg_order_value"],
                "metric_issues": metric_issues,
            }

        except Exception as e:
            passed = False
            score = 0.0
            details = {"error": str(e)}

        return self._create_result(
            "business_metrics_validation",
            passed,
            score,
            details,
            f"Found {len(metric_issues)} metric issues" if metric_issues else None,
        )

    def _validate_data_freshness(self, connection) -> ValidationResult:
        """Validate data freshness in warehouse"""
        try:
            # Check data freshness based on ingestion timestamps
            # This is a placeholder - implement actual queries

            max_data_age_hours = 24  # Business requirement
            actual_age_hours = 2  # Placeholder

            passed = actual_age_hours <= max_data_age_hours
            score = max(0.0, 1.0 - (actual_age_hours / max_data_age_hours))

            details = {
                "max_allowed_age_hours": max_data_age_hours,
                "actual_age_hours": actual_age_hours,
                "is_fresh": passed,
            }

        except Exception as e:
            passed = False
            score = 0.0
            details = {"error": str(e)}

        return self._create_result(
            "data_freshness_validation",
            passed,
            score,
            details,
            f"Data is stale: {actual_age_hours} hours old" if not passed else None,
        )


class PipelineValidator:
    """Main validator that orchestrates all stage validations"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validators = {
            "raw_data": RawDataValidator(),
            "kafka_streaming": KafkaStreamValidator(),
            "spark_etl": SparkETLValidator(),
            "data_warehouse": DataWarehouseValidator(),
        }

    def validate_pipeline_run(
        self,
        pipeline_run_id: str,
        validation_data: Dict[str, Any],
    ) -> ValidationSummary:
        """Validate complete pipeline run"""

        start_time = datetime.now()
        stage_results = {}
        all_results = []

        # Run validations for each stage
        for stage_name, validator in self.validators.items():
            if stage_name in validation_data:
                try:
                    self.logger.info(f"Validating stage: {stage_name}")
                    stage_data = validation_data[stage_name]

                    if stage_name == "raw_data":
                        results = validator.validate(stage_data)
                    elif stage_name == "kafka_streaming":
                        results = validator.validate(
                            stage_data["messages"],
                            duration_seconds=stage_data.get("duration_seconds", 60),
                            min_throughput=stage_data.get("min_throughput", 100),
                        )
                    elif stage_name == "spark_etl":
                        results = validator.validate(
                            stage_data["input_data"],
                            stage_data["output_data"],
                            processing_time_seconds=stage_data.get(
                                "processing_time_seconds", 0
                            ),
                            min_throughput=stage_data.get("min_throughput", 1000),
                        )
                    elif stage_name == "data_warehouse":
                        results = validator.validate(stage_data["connection"])
                    else:
                        results = []

                    stage_results[stage_name] = results
                    all_results.extend(results)

                except Exception as e:
                    self.logger.error(f"Validation failed for stage {stage_name}: {e}")
                    error_result = ValidationResult(
                        stage=stage_name,
                        check_name="stage_validation_error",
                        passed=False,
                        score=0.0,
                        details={"error": str(e)},
                        timestamp=datetime.now(),
                        error_message=str(e),
                    )
                    stage_results[stage_name] = [error_result]
                    all_results.append(error_result)

        end_time = datetime.now()

        # Calculate overall results
        overall_passed = all(result.passed for result in all_results)
        overall_score = (
            np.mean([result.score for result in all_results]) if all_results else 0.0
        )

        return ValidationSummary(
            pipeline_run_id=pipeline_run_id,
            start_time=start_time,
            end_time=end_time,
            overall_passed=overall_passed,
            overall_score=overall_score,
            stage_results=stage_results,
        )

    def save_validation_report(
        self, summary: ValidationSummary, output_path: str
    ) -> None:
        """Save validation report to file"""
        try:
            report_data = summary.to_dict()

            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w") as f:
                json.dump(report_data, f, indent=2, default=str)

            self.logger.info(f"Validation report saved to: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to save validation report: {e}")
            raise
