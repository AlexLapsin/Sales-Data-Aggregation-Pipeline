#!/usr/bin/env python3
"""
Test Data Generator for End-to-End Pipeline Testing

This module generates realistic test data for all stages of the sales pipeline,
including CSV files, Kafka messages, and expected outputs for validation.
"""

import csv
import json
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from dataclasses import dataclass, field


@dataclass
class DataGenerationConfig:
    """Configuration for test data generation"""

    # Time range for generated data
    start_date: datetime = field(
        default_factory=lambda: datetime.now() - timedelta(days=30)
    )
    end_date: datetime = field(default_factory=lambda: datetime.now())

    # Data distribution parameters
    num_customers: int = 1000
    num_products: int = 500
    num_stores: int = 50

    # Business parameters
    avg_orders_per_day: int = 500
    avg_items_per_order: float = 2.5
    seasonal_factor: float = 1.2

    # Quality parameters (for testing data quality issues)
    error_rate: float = 0.02  # 2% error rate for testing validation
    duplicate_rate: float = 0.01  # 1% duplicate rate
    null_rate: float = 0.005  # 0.5% null rate


class TestDataGenerator:
    """Generates comprehensive test data for pipeline testing"""

    def __init__(self, config: Optional[DataGenerationConfig] = None):
        self.config = config or DataGenerationConfig()
        self.logger = logging.getLogger(__name__)

        # Initialize data references
        self._init_reference_data()

    def _init_reference_data(self):
        """Initialize reference data for realistic generation"""

        self.categories = {
            "Electronics": {
                "products": [
                    "Smartphone",
                    "Laptop",
                    "Tablet",
                    "Headphones",
                    "Smartwatch",
                    "Desktop Computer",
                    "Monitor",
                    "Keyboard",
                    "Mouse",
                    "Speaker",
                    "TV",
                    "Gaming Console",
                    "Camera",
                    "Printer",
                    "Router",
                ],
                "price_range": (50, 2000),
                "avg_margin": 0.15,
            },
            "Clothing": {
                "products": [
                    "T-Shirt",
                    "Jeans",
                    "Dress",
                    "Sneakers",
                    "Jacket",
                    "Sweater",
                    "Shorts",
                    "Skirt",
                    "Boots",
                    "Sandals",
                    "Blouse",
                    "Pants",
                    "Hoodie",
                    "Coat",
                    "Socks",
                ],
                "price_range": (15, 200),
                "avg_margin": 0.50,
            },
            "Home & Garden": {
                "products": [
                    "Sofa",
                    "Dining Table",
                    "Lamp",
                    "Rug",
                    "Mirror",
                    "Chair",
                    "Bookshelf",
                    "Curtains",
                    "Vase",
                    "Plant",
                    "Tool Set",
                    "Garden Hose",
                    "Lawn Mower",
                    "Grill",
                    "Patio Set",
                ],
                "price_range": (25, 800),
                "avg_margin": 0.35,
            },
            "Books": {
                "products": [
                    "Fiction Novel",
                    "Biography",
                    "Textbook",
                    "Cookbook",
                    "Manual",
                    "Children's Book",
                    "Self-Help",
                    "History",
                    "Science",
                    "Art Book",
                    "Travel Guide",
                    "Poetry",
                    "Dictionary",
                    "Encyclopedia",
                    "Journal",
                ],
                "price_range": (5, 60),
                "avg_margin": 0.40,
            },
            "Sports": {
                "products": [
                    "Running Shoes",
                    "Gym Equipment",
                    "Sports Apparel",
                    "Basketball",
                    "Tennis Racket",
                    "Golf Clubs",
                    "Bicycle",
                    "Helmet",
                    "Water Bottle",
                    "Yoga Mat",
                    "Dumbbells",
                    "Treadmill",
                    "Soccer Ball",
                    "Baseball Glove",
                    "Ski Equipment",
                ],
                "price_range": (10, 500),
                "avg_margin": 0.30,
            },
        }

        self.regions = {
            "North America": {
                "countries": ["United States", "Canada", "Mexico"],
                "states": ["California", "Texas", "New York", "Florida", "Illinois"],
                "cities": [
                    "Los Angeles",
                    "Chicago",
                    "Houston",
                    "Phoenix",
                    "Philadelphia",
                ],
            },
            "Europe": {
                "countries": ["United Kingdom", "Germany", "France", "Italy", "Spain"],
                "states": [
                    "England",
                    "Bavaria",
                    "Ile-de-France",
                    "Lombardy",
                    "Catalonia",
                ],
                "cities": ["London", "Berlin", "Paris", "Milan", "Barcelona"],
            },
            "Asia Pacific": {
                "countries": [
                    "Japan",
                    "Australia",
                    "South Korea",
                    "Singapore",
                    "India",
                ],
                "states": [
                    "Tokyo",
                    "New South Wales",
                    "Seoul",
                    "Central",
                    "Maharashtra",
                ],
                "cities": ["Tokyo", "Sydney", "Seoul", "Singapore", "Mumbai"],
            },
        }

        self.customer_segments = ["Consumer", "Corporate", "Home Office"]
        self.ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]

        # Generate consistent customer, product, and store IDs
        self.customer_ids = [
            f"CUST_{i:05d}" for i in range(1, self.config.num_customers + 1)
        ]
        self.product_ids = self._generate_product_ids()
        self.store_ids = [
            f"STORE_{i:03d}" for i in range(1, self.config.num_stores + 1)
        ]

    def _generate_product_ids(self) -> List[str]:
        """Generate realistic product IDs"""
        product_ids = []
        for category, category_data in self.categories.items():
            for i, product in enumerate(category_data["products"]):
                product_id = f"{category.upper().replace(' ', '').replace('&', '')}_{product.upper().replace(' ', '_')}_{i+1:03d}"
                product_ids.append(product_id)
        return product_ids

    def generate_csv_data(
        self, num_records: int, output_path: Optional[str] = None
    ) -> pd.DataFrame:
        """Generate realistic CSV sales data"""

        self.logger.info(f"Generating {num_records} CSV records")

        records = []
        order_counter = 1

        # Calculate daily distribution
        total_days = (self.config.end_date - self.config.start_date).days
        if total_days <= 0:
            total_days = 1

        records_per_day = num_records // total_days
        remaining_records = num_records % total_days

        for day in range(total_days):
            current_date = self.config.start_date + timedelta(days=day)

            # Add seasonal variation
            seasonal_multiplier = 1 + 0.3 * np.sin(2 * np.pi * day / 365)

            # Calculate records for this day
            day_records = records_per_day
            if day < remaining_records:
                day_records += 1

            day_records = int(day_records * seasonal_multiplier)

            for _ in range(day_records):
                record = self._generate_single_csv_record(current_date, order_counter)
                records.append(record)
                order_counter += 1

        # Introduce data quality issues for testing
        records = self._introduce_data_quality_issues(records)

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Save to file if path provided
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_file, index=False)
            self.logger.info(f"CSV data saved to: {output_path}")

        return df

    def _generate_single_csv_record(
        self, order_date: datetime, order_id: int
    ) -> Dict[str, Any]:
        """Generate a single CSV record"""

        # Select random category and product
        category = random.choice(list(self.categories.keys()))
        product = random.choice(self.categories[category]["products"])
        price_range = self.categories[category]["price_range"]
        margin = self.categories[category]["avg_margin"]

        # Generate order details
        quantity = random.choices(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            weights=[40, 25, 15, 8, 5, 3, 2, 1, 0.5, 0.5],
        )[0]

        unit_price = round(random.uniform(*price_range), 2)
        sales = round(unit_price * quantity, 2)

        # Calculate profit with some variation
        base_profit = sales * margin
        profit_variation = random.uniform(0.8, 1.2)
        profit = round(base_profit * profit_variation, 2)

        # Generate discount (30% chance of discount)
        discount = 0.0
        if random.random() < 0.3:
            discount = round(random.uniform(0.05, 0.25), 2)
            sales = round(sales * (1 - discount), 2)
            profit = round(profit * (1 - discount), 2)

        # Select region and location
        region = random.choice(list(self.regions.keys()))
        region_data = self.regions[region]
        country = random.choice(region_data["countries"])
        state = random.choice(region_data["states"])
        city = random.choice(region_data["cities"])

        # Generate shipping date (1-7 days after order)
        ship_date = order_date + timedelta(days=random.randint(1, 7))

        # Create record
        record = {
            "Row ID": order_id,
            "Order ID": f"ORDER_{order_id:08d}",
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": ship_date.strftime("%m/%d/%Y"),
            "Ship Mode": random.choice(self.ship_modes),
            "Customer ID": random.choice(self.customer_ids),
            "Customer Name": f"Customer {random.randint(1, self.config.num_customers)}",
            "Segment": random.choice(self.customer_segments),
            "Country": country,
            "City": city,
            "State": state,
            "Postal Code": str(random.randint(10000, 99999)),
            "Region": region,
            "Product ID": f"{category.upper().replace(' ', '').replace('&', '')}_{product.upper().replace(' ', '_')}_{random.randint(1, 999):03d}",
            "Category": category,
            "Sub-Category": product,
            "Product Name": f"{product} Model {random.randint(100, 999)}",
            "Sales": sales,
            "Quantity": quantity,
            "Discount": discount,
            "Profit": profit,
        }

        return record

    def _introduce_data_quality_issues(self, records: List[Dict]) -> List[Dict]:
        """Introduce controlled data quality issues for testing"""

        modified_records = records.copy()
        num_records = len(records)

        # Introduce null values
        null_count = int(num_records * self.config.null_rate)
        for _ in range(null_count):
            record_idx = random.randint(0, num_records - 1)
            nullable_fields = ["Customer Name", "Postal Code", "Sub-Category"]
            field = random.choice(nullable_fields)
            modified_records[record_idx][field] = None

        # Introduce duplicates
        duplicate_count = int(num_records * self.config.duplicate_rate)
        for _ in range(duplicate_count):
            source_idx = random.randint(0, num_records - 1)
            duplicate_record = modified_records[source_idx].copy()
            # Slightly modify the duplicate to make it realistic
            duplicate_record["Row ID"] = len(modified_records) + 1
            modified_records.append(duplicate_record)

        # Introduce data errors
        error_count = int(num_records * self.config.error_rate)
        for _ in range(error_count):
            record_idx = random.randint(0, len(modified_records) - 1)
            error_type = random.choice(
                ["negative_sales", "zero_quantity", "future_date", "invalid_discount"]
            )

            if error_type == "negative_sales":
                modified_records[record_idx]["Sales"] = -abs(
                    modified_records[record_idx]["Sales"]
                )
            elif error_type == "zero_quantity":
                modified_records[record_idx]["Quantity"] = 0
            elif error_type == "future_date":
                future_date = datetime.now() + timedelta(days=random.randint(1, 365))
                modified_records[record_idx]["Order Date"] = future_date.strftime(
                    "%m/%d/%Y"
                )
            elif error_type == "invalid_discount":
                modified_records[record_idx]["Discount"] = random.uniform(
                    1.1, 2.0
                )  # Invalid discount > 100%

        return modified_records

    def generate_kafka_messages(
        self, num_messages: int, duration_seconds: int = 60
    ) -> List[Dict]:
        """Generate Kafka streaming messages"""

        self.logger.info(
            f"Generating {num_messages} Kafka messages over {duration_seconds} seconds"
        )

        messages = []
        start_time = datetime.now(timezone.utc)

        for i in range(num_messages):
            # Distribute messages over time duration
            timestamp_offset = (i / num_messages) * duration_seconds
            message_timestamp = start_time + timedelta(seconds=timestamp_offset)

            message = self._generate_single_kafka_message(message_timestamp)
            messages.append(message)

        # Introduce some message quality issues for testing
        messages = self._introduce_kafka_message_issues(messages)

        return messages

    def _generate_single_kafka_message(self, timestamp: datetime) -> Dict[str, Any]:
        """Generate a single Kafka message"""

        # Select category and product
        category = random.choice(list(self.categories.keys()))
        product = random.choice(self.categories[category]["products"])
        price_range = self.categories[category]["price_range"]

        # Generate sales data
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]

        unit_price = round(random.uniform(*price_range), 2)
        total_price = round(unit_price * quantity, 2)

        # Apply discount
        discount = 0.0
        if random.random() < 0.3:
            discount = round(random.uniform(0.05, 0.25), 2)
            total_price = round(total_price * (1 - discount), 2)

        # Select store location
        region = random.choice(list(self.regions.keys()))
        region_data = self.regions[region]
        store_location = random.choice(region_data["cities"])

        message = {
            "order_id": str(uuid.uuid4()),
            "store_id": random.choice(self.store_ids),
            "product_id": f"{category.upper().replace(' ', '').replace('&', '')}_{product.upper().replace(' ', '_')}_{random.randint(1, 999):03d}",
            "product_name": f"{product} Model {random.randint(100, 999)}",
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "sale_timestamp": timestamp.isoformat(),
            "customer_id": random.choice(self.customer_ids),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "Cash", "Digital Wallet"]
            ),
            "discount": discount,
            "store_location": store_location,
        }

        return message

    def _introduce_kafka_message_issues(self, messages: List[Dict]) -> List[Dict]:
        """Introduce message quality issues for testing"""

        modified_messages = messages.copy()
        num_messages = len(messages)

        # Introduce missing fields
        missing_field_count = int(num_messages * 0.01)  # 1% missing fields
        for _ in range(missing_field_count):
            msg_idx = random.randint(0, num_messages - 1)
            field_to_remove = random.choice(
                ["payment_method", "discount", "store_location"]
            )
            if field_to_remove in modified_messages[msg_idx]:
                del modified_messages[msg_idx][field_to_remove]

        # Introduce duplicate order IDs
        duplicate_count = int(num_messages * 0.005)  # 0.5% duplicates
        for _ in range(duplicate_count):
            if len(modified_messages) >= 2:
                source_idx = random.randint(0, len(modified_messages) - 1)
                target_idx = random.randint(0, len(modified_messages) - 1)
                if source_idx != target_idx:
                    modified_messages[target_idx]["order_id"] = modified_messages[
                        source_idx
                    ]["order_id"]

        # Introduce malformed messages
        malformed_count = int(num_messages * 0.002)  # 0.2% malformed
        for _ in range(malformed_count):
            msg_idx = random.randint(0, len(modified_messages) - 1)
            # Make price negative or quantity zero
            error_type = random.choice(
                ["negative_price", "zero_quantity", "null_values"]
            )

            if error_type == "negative_price":
                modified_messages[msg_idx]["total_price"] = -abs(
                    modified_messages[msg_idx]["total_price"]
                )
            elif error_type == "zero_quantity":
                modified_messages[msg_idx]["quantity"] = 0
            elif error_type == "null_values":
                modified_messages[msg_idx]["product_id"] = None

        return modified_messages

    def generate_expected_spark_output(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Generate expected Spark ETL output based on input data"""

        self.logger.info(
            f"Generating expected Spark output for {len(input_df)} input records"
        )

        # Start with input data
        output_df = input_df.copy()

        # Apply expected transformations

        # 1. Filter out invalid records
        output_df = output_df[
            (output_df["Sales"] >= 0)
            & (output_df["Quantity"] > 0)
            & (pd.notna(output_df["Order ID"]))
        ]

        # 2. Standardize text fields
        output_df["Category"] = output_df["Category"].str.upper()
        output_df["Region"] = output_df["Region"].str.upper()
        output_df["Country"] = output_df["Country"].str.upper()
        output_df["State"] = output_df["State"].str.upper()

        # 3. Handle discounts (convert percentages > 1 to decimals)
        output_df.loc[output_df["Discount"] > 1, "Discount"] = (
            output_df["Discount"] / 100
        )

        # 4. Calculate derived fields
        output_df["unit_price"] = output_df["Sales"] / output_df["Quantity"]
        output_df["profit_margin"] = output_df["Profit"] / output_df["Sales"]
        output_df["profit_margin"] = output_df["profit_margin"].fillna(0)

        # 5. Add metadata fields
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        output_df["ingestion_timestamp"] = datetime.now()
        output_df["source_system"] = "BATCH"
        output_df["batch_id"] = batch_id
        output_df["partition_date"] = datetime.now().strftime("%Y-%m-%d")

        # 6. Map to Snowflake schema
        column_mapping = {
            "Order ID": "ORDER_ID",
            "Customer ID": "STORE_ID",  # Using Customer ID as Store ID for compatibility
            "Product ID": "PRODUCT_ID",
            "Product Name": "PRODUCT_NAME",
            "Category": "CATEGORY",
            "Quantity": "QUANTITY",
            "unit_price": "UNIT_PRICE",
            "Sales": "TOTAL_PRICE",
            "Order Date": "ORDER_DATE",
            "Ship Date": "SHIP_DATE",
            "Sales": "SALES",
            "Profit": "PROFIT",
            "Segment": "CUSTOMER_SEGMENT",
            "Region": "REGION",
            "Country": "COUNTRY",
            "State": "STATE",
            "City": "CITY",
            "batch_id": "BATCH_ID",
            "ingestion_timestamp": "INGESTION_TIMESTAMP",
            "source_system": "SOURCE_SYSTEM",
            "partition_date": "PARTITION_DATE",
        }

        # Select and rename columns
        available_columns = {
            old: new for old, new in column_mapping.items() if old in output_df.columns
        }
        output_df = output_df[list(available_columns.keys())].rename(
            columns=available_columns
        )

        # Add missing columns with default values
        expected_columns = set(column_mapping.values())
        current_columns = set(output_df.columns)
        missing_columns = expected_columns - current_columns

        for col in missing_columns:
            if col == "SOURCE_FILE":
                output_df[col] = "test_file.csv"
            else:
                output_df[col] = None

        return output_df

    def generate_test_scenario_data(
        self, scenario_name: str, **kwargs
    ) -> Dict[str, Any]:
        """Generate data for specific test scenarios"""

        scenarios = {
            "happy_path": self._generate_happy_path_data,
            "data_quality_issues": self._generate_data_quality_scenario,
            "high_volume": self._generate_high_volume_scenario,
            "edge_cases": self._generate_edge_case_scenario,
            "performance_test": self._generate_performance_scenario,
        }

        if scenario_name not in scenarios:
            raise ValueError(
                f"Unknown scenario: {scenario_name}. Available: {list(scenarios.keys())}"
            )

        self.logger.info(f"Generating test data for scenario: {scenario_name}")
        return scenarios[scenario_name](**kwargs)

    def _generate_happy_path_data(self, record_count: int = 1000) -> Dict[str, Any]:
        """Generate clean, perfect data for happy path testing"""

        # Temporarily reduce error rates for clean data
        original_error_rate = self.config.error_rate
        original_duplicate_rate = self.config.duplicate_rate
        original_null_rate = self.config.null_rate

        self.config.error_rate = 0.0
        self.config.duplicate_rate = 0.0
        self.config.null_rate = 0.0

        try:
            csv_data = self.generate_csv_data(record_count)
            kafka_messages = self.generate_kafka_messages(record_count // 10, 60)
            expected_output = self.generate_expected_spark_output(csv_data)

            return {
                "csv_data": csv_data,
                "kafka_messages": kafka_messages,
                "expected_spark_output": expected_output,
                "scenario_description": "Clean data with no quality issues",
            }
        finally:
            # Restore original error rates
            self.config.error_rate = original_error_rate
            self.config.duplicate_rate = original_duplicate_rate
            self.config.null_rate = original_null_rate

    def _generate_data_quality_scenario(
        self, record_count: int = 1000
    ) -> Dict[str, Any]:
        """Generate data with intentional quality issues"""

        # Increase error rates for testing data quality handling
        self.config.error_rate = 0.1  # 10% error rate
        self.config.duplicate_rate = 0.05  # 5% duplicate rate
        self.config.null_rate = 0.03  # 3% null rate

        csv_data = self.generate_csv_data(record_count)
        kafka_messages = self.generate_kafka_messages(record_count // 10, 60)
        expected_output = self.generate_expected_spark_output(csv_data)

        return {
            "csv_data": csv_data,
            "kafka_messages": kafka_messages,
            "expected_spark_output": expected_output,
            "scenario_description": "Data with intentional quality issues for validation testing",
        }

    def _generate_high_volume_scenario(
        self, record_count: int = 50000
    ) -> Dict[str, Any]:
        """Generate high volume data for scalability testing"""

        csv_data = self.generate_csv_data(record_count)
        kafka_messages = self.generate_kafka_messages(
            record_count // 5, 300
        )  # 5 minutes of messages
        expected_output = self.generate_expected_spark_output(csv_data)

        return {
            "csv_data": csv_data,
            "kafka_messages": kafka_messages,
            "expected_spark_output": expected_output,
            "scenario_description": "High volume data for scalability and performance testing",
        }

    def _generate_edge_case_scenario(self, record_count: int = 500) -> Dict[str, Any]:
        """Generate edge case data for robustness testing"""

        # Generate base data
        csv_data = self.generate_csv_data(record_count)

        # Add specific edge cases
        edge_cases = []

        # Very large sales amounts
        edge_cases.append(
            {
                "Row ID": len(csv_data) + 1,
                "Order ID": "EDGE_LARGE_SALE",
                "Sales": 999999.99,
                "Quantity": 1,
                "Profit": 100000.00,
                "Category": "Electronics",
                "Order Date": datetime.now().strftime("%m/%d/%Y"),
                "Ship Date": (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y"),
            }
        )

        # Very small sales amounts
        edge_cases.append(
            {
                "Row ID": len(csv_data) + 2,
                "Order ID": "EDGE_SMALL_SALE",
                "Sales": 0.01,
                "Quantity": 1,
                "Profit": 0.001,
                "Category": "Books",
                "Order Date": datetime.now().strftime("%m/%d/%Y"),
                "Ship Date": (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y"),
            }
        )

        # Very long text fields
        edge_cases.append(
            {
                "Row ID": len(csv_data) + 3,
                "Order ID": "EDGE_LONG_TEXT",
                "Product Name": "A" * 255,  # Very long product name
                "Customer Name": "B" * 100,  # Very long customer name
                "Sales": 100.00,
                "Quantity": 1,
                "Profit": 10.00,
                "Category": "Clothing",
                "Order Date": datetime.now().strftime("%m/%d/%Y"),
                "Ship Date": (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y"),
            }
        )

        # Add edge cases to CSV data
        csv_df = pd.concat([csv_data, pd.DataFrame(edge_cases)], ignore_index=True)

        kafka_messages = self.generate_kafka_messages(record_count // 10, 60)
        expected_output = self.generate_expected_spark_output(csv_df)

        return {
            "csv_data": csv_df,
            "kafka_messages": kafka_messages,
            "expected_spark_output": expected_output,
            "scenario_description": "Edge cases for robustness testing",
        }

    def _generate_performance_scenario(
        self, record_count: int = 100000
    ) -> Dict[str, Any]:
        """Generate data specifically for performance benchmarking"""

        csv_data = self.generate_csv_data(record_count)
        kafka_messages = self.generate_kafka_messages(
            record_count // 2, 600
        )  # 10 minutes of messages
        expected_output = self.generate_expected_spark_output(csv_data)

        return {
            "csv_data": csv_data,
            "kafka_messages": kafka_messages,
            "expected_spark_output": expected_output,
            "scenario_description": "Large dataset for performance benchmarking",
            "performance_metrics": {
                "expected_processing_time_seconds": record_count
                / 1000,  # Rough estimate
                "expected_memory_usage_mb": record_count * 0.1,  # Rough estimate
                "expected_throughput_records_per_second": 1000,
            },
        }

    def save_test_data(
        self, data: Dict[str, Any], output_dir: str, scenario_name: str
    ) -> Dict[str, str]:
        """Save generated test data to files"""

        output_path = Path(output_dir) / scenario_name
        output_path.mkdir(parents=True, exist_ok=True)

        file_paths = {}

        # Save CSV data
        if "csv_data" in data:
            csv_path = output_path / "input_data.csv"
            data["csv_data"].to_csv(csv_path, index=False)
            file_paths["csv_data"] = str(csv_path)

        # Save Kafka messages
        if "kafka_messages" in data:
            kafka_path = output_path / "kafka_messages.json"
            with open(kafka_path, "w") as f:
                json.dump(data["kafka_messages"], f, indent=2, default=str)
            file_paths["kafka_messages"] = str(kafka_path)

        # Save expected output
        if "expected_spark_output" in data:
            output_path_file = output_path / "expected_output.csv"
            data["expected_spark_output"].to_csv(output_path_file, index=False)
            file_paths["expected_output"] = str(output_path_file)

        # Save metadata
        metadata = {
            "scenario_name": scenario_name,
            "generation_timestamp": datetime.now().isoformat(),
            "record_counts": {
                "csv_records": len(data.get("csv_data", [])),
                "kafka_messages": len(data.get("kafka_messages", [])),
                "expected_output_records": len(data.get("expected_spark_output", [])),
            },
            "scenario_description": data.get("scenario_description", ""),
            "file_paths": file_paths,
        }

        metadata_path = output_path / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
        file_paths["metadata"] = str(metadata_path)

        self.logger.info(f"Test data saved to: {output_path}")
        return file_paths


class MockComponentGenerator:
    """Generates mock components for testing"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def generate_mock_kafka_config(self, port: int = 29092) -> Dict[str, Any]:
        """Generate mock Kafka configuration"""
        return {
            "bootstrap_servers": f"localhost:{port}",
            "topic": "test_sales_events",
            "group_id": "test_consumer_group",
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
        }

    def generate_mock_database_config(self, port: int = 15432) -> Dict[str, Any]:
        """Generate mock database configuration"""
        return {
            "host": "localhost",
            "port": port,
            "database": "test_sales_pipeline",
            "user": "test_user",
            "password": "test_password",
            "options": "-c search_path=public",
        }

    def generate_mock_s3_config(self, port: int = 19000) -> Dict[str, Any]:
        """Generate mock S3/MinIO configuration"""
        return {
            "endpoint_url": f"http://localhost:{port}",
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "bucket_name": "test-sales-data",
            "processed_bucket_name": "test-processed-sales",
            "region_name": "us-east-1",
        }

    def generate_docker_compose_config(self) -> Dict[str, Any]:
        """Generate Docker Compose configuration for test infrastructure"""
        return {
            "version": "3.8",
            "services": {
                "zookeeper": {
                    "image": "confluentinc/cp-zookeeper:7.4.0",
                    "hostname": "zookeeper",
                    "container_name": "test-zookeeper",
                    "ports": ["22181:2181"],
                    "environment": {
                        "ZOOKEEPER_CLIENT_PORT": 2181,
                        "ZOOKEEPER_TICK_TIME": 2000,
                    },
                },
                "kafka": {
                    "image": "confluentinc/cp-kafka:7.4.0",
                    "hostname": "kafka",
                    "container_name": "test-kafka",
                    "depends_on": ["zookeeper"],
                    "ports": ["29092:29092"],
                    "environment": {
                        "KAFKA_BROKER_ID": 1,
                        "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
                        "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092",
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 1,
                        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 1,
                        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 1,
                        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
                    },
                },
                "postgres": {
                    "image": "postgres:15",
                    "container_name": "test-postgres",
                    "ports": ["15432:5432"],
                    "environment": {
                        "POSTGRES_DB": "test_sales_pipeline",
                        "POSTGRES_USER": "test_user",
                        "POSTGRES_PASSWORD": "test_password",
                    },
                    "volumes": ["postgres_data:/var/lib/postgresql/data"],
                },
                "minio": {
                    "image": "minio/minio:RELEASE.2023-07-07T07-13-57Z",
                    "container_name": "test-minio",
                    "ports": ["19000:9000", "19001:9001"],
                    "environment": {
                        "MINIO_ROOT_USER": "minioadmin",
                        "MINIO_ROOT_PASSWORD": "minioadmin",
                    },
                    "command": "server /data --console-address ':9001'",
                    "volumes": ["minio_data:/data"],
                },
            },
            "volumes": {
                "postgres_data": {},
                "minio_data": {},
            },
            "networks": {
                "default": {
                    "name": "sales-pipeline-test-network",
                }
            },
        }
