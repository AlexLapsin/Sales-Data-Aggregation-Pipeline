#!/usr/bin/env python3
"""
Kafka Producer for Sales Data Simulation

This script produces streaming sales events to a Kafka topic to simulate
real-time sales data ingestion. It can work with both local Docker Kafka
and AWS MSK clusters.

Usage:
    python kafka_producer.py [--config config.json] [--topic sales_events] [--count 1000]
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, Optional
import argparse

from kafka import KafkaProducer
from kafka.errors import KafkaError
import uuid


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SalesDataGenerator:
    """Generates realistic sales data for streaming simulation"""

    def __init__(self):
        # Sample data for realistic simulation
        self.product_categories = {
            "Electronics": [
                "Smartphone",
                "Laptop",
                "Tablet",
                "Headphones",
                "Smartwatch",
            ],
            "Clothing": ["Shirt", "Jeans", "Dress", "Shoes", "Jacket"],
            "Home": ["Sofa", "Table", "Lamp", "Rug", "Mirror"],
            "Books": ["Fiction", "Biography", "Textbook", "Cookbook", "Manual"],
            "Sports": [
                "Running Shoes",
                "Gym Equipment",
                "Sports Apparel",
                "Ball",
                "Racket",
            ],
        }

        # Price ranges by category
        self.price_ranges = {
            "Electronics": (50, 2000),
            "Clothing": (15, 200),
            "Home": (25, 800),
            "Books": (5, 60),
            "Sports": (10, 300),
        }

        # Geographic data pools with consistent Market/Region/Country/State mappings
        self.geographic_pools = {
            "US": {
                "market": "US",
                "regions": {
                    "East": {
                        "states": [
                            "New York",
                            "Pennsylvania",
                            "Florida",
                            "Georgia",
                            "Virginia",
                        ],
                        "cities": [
                            "New York",
                            "Philadelphia",
                            "Miami",
                            "Atlanta",
                            "Richmond",
                        ],
                    },
                    "West": {
                        "states": [
                            "California",
                            "Washington",
                            "Oregon",
                            "Nevada",
                            "Arizona",
                        ],
                        "cities": [
                            "Los Angeles",
                            "Seattle",
                            "Portland",
                            "Las Vegas",
                            "Phoenix",
                        ],
                    },
                    "Central": {
                        "states": [
                            "Illinois",
                            "Ohio",
                            "Michigan",
                            "Indiana",
                            "Wisconsin",
                        ],
                        "cities": [
                            "Chicago",
                            "Columbus",
                            "Detroit",
                            "Indianapolis",
                            "Milwaukee",
                        ],
                    },
                    "South": {
                        "states": [
                            "Texas",
                            "Louisiana",
                            "Tennessee",
                            "Kentucky",
                            "Alabama",
                        ],
                        "cities": [
                            "Houston",
                            "New Orleans",
                            "Nashville",
                            "Louisville",
                            "Birmingham",
                        ],
                    },
                },
                "country": "United States",
            },
            "Canada": {
                "market": "Canada",
                "regions": {
                    "Canada": {
                        "provinces": ["Ontario", "Quebec", "British Columbia"],
                        "cities": ["Toronto", "Montreal", "Vancouver"],
                    }
                },
                "country": "Canada",
            },
            "EU": {
                "market": "EU",
                "regions": {
                    "North": {
                        "countries": ["United Kingdom", "Germany", "France"],
                        "cities": ["London", "Berlin", "Paris"],
                    },
                    "South": {
                        "countries": ["Italy", "Spain", "Greece"],
                        "cities": ["Rome", "Madrid", "Athens"],
                    },
                },
            },
            "APAC": {
                "market": "APAC",
                "regions": {
                    "Southeast Asia": {
                        "countries": ["Singapore", "Malaysia", "Thailand"],
                        "cities": ["Singapore", "Kuala Lumpur", "Bangkok"],
                    },
                    "East Asia": {
                        "countries": ["Japan", "South Korea", "China"],
                        "cities": ["Tokyo", "Seoul", "Shanghai"],
                    },
                },
            },
            "LATAM": {
                "market": "LATAM",
                "regions": {
                    "South": {
                        "countries": ["Brazil", "Argentina", "Chile"],
                        "cities": ["São Paulo", "Buenos Aires", "Santiago"],
                    }
                },
            },
            "Africa": {
                "market": "Africa",
                "regions": {
                    "Africa": {
                        "countries": ["South Africa", "Egypt", "Nigeria"],
                        "cities": ["Cape Town", "Cairo", "Lagos"],
                    }
                },
            },
        }

    def generate_sales_event(self) -> Dict:
        """Generate a complete 24-field sales event with geographically consistent data"""
        from faker import Faker

        fake = Faker()

        # Generate base data
        category = random.choice(list(self.product_categories.keys()))
        product = random.choice(self.product_categories[category])
        price_range = self.price_ranges[category]

        # Generate realistic quantities with weighted distribution
        quantity = random.choices(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            weights=[40, 25, 15, 8, 5, 3, 2, 1, 0.5, 0.5],
        )[0]

        unit_price = round(random.uniform(*price_range), 2)
        sales_amount = round(unit_price * quantity, 2)
        discount_rate = (
            round(random.uniform(0, 0.5), 4) if random.random() < 0.3 else 0.0
        )
        shipping_cost = round(random.uniform(5.0, 50.0), 2)
        profit = round(sales_amount * random.uniform(0.1, 0.4) - shipping_cost, 2)

        # Generate geographically consistent location data
        market_key = random.choice(list(self.geographic_pools.keys()))
        geo_pool = self.geographic_pools[market_key]
        market = geo_pool["market"]

        # Select region within market
        region_key = random.choice(list(geo_pool["regions"].keys()))
        region_data = geo_pool["regions"][region_key]
        region = region_key

        # Generate location based on market type
        if market_key == "US":
            # US: has states and cities
            state = random.choice(region_data["states"])
            city = random.choice(region_data["cities"])
            country = geo_pool["country"]
            postal_code = fake.postcode()
        elif market_key == "Canada":
            # Canada: use provinces as "state" field (matches Kaggle schema)
            state = random.choice(region_data["provinces"])
            city = random.choice(region_data["cities"])
            country = geo_pool["country"]
            postal_code = fake.postcode()
        else:
            # International: State is NULL, use country from pool
            state = None
            country = random.choice(region_data["countries"])
            city = random.choice(region_data["cities"])
            postal_code = None  # Many international orders lack postal codes

        # Generate complete 24-field event matching CSV schema
        event = {
            # Core transaction fields (1-6)
            "row_id": random.randint(1, 1000000),
            "order_id": f"ORD-{random.randint(100000, 999999)}",
            "order_date": fake.date_between(
                start_date="-30d", end_date="today"
            ).strftime("%d-%m-%Y"),
            "ship_date": fake.date_between(
                start_date="today", end_date="+10d"
            ).strftime("%d-%m-%Y"),
            "ship_mode": random.choice(
                ["Same Day", "First Class", "Second Class", "Standard Class"]
            ),
            # Customer fields (7-12)
            "customer_id": f"CUST-{random.randint(10000, 99999)}",
            "customer_name": fake.name(),
            "segment": random.choice(["Consumer", "Corporate", "Home Office"]),
            "city": city,
            "state": state,  # NULL for non-US/Canada countries
            "country": country,
            "postal_code": postal_code,
            # Geographic fields (13-14) - now consistent!
            "market": market,
            "region": region,
            # Product fields (15-18)
            "product_id": f"PROD-{random.randint(1000, 9999)}",
            "category": category,
            "sub_category": random.choice(
                [
                    "Phones",
                    "Chairs",
                    "Storage",
                    "Tables",
                    "Binders",
                    "Supplies",
                    "Machines",
                ]
            ),
            "product_name": product,
            # Financial fields (19-23)
            "sales": sales_amount,
            "quantity": quantity,
            "discount": discount_rate,
            "profit": profit,
            "shipping_cost": shipping_cost,
            # Business priority (24)
            "order_priority": random.choice(["Low", "Medium", "High", "Critical"]),
            # Streaming metadata (additional fields for processing)
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_system": "streaming",
            "producer_id": "kafka_sales_producer_v3_geo_fixed",
        }

        return event


class KafkaSalesProducer:
    """Kafka producer for sales data streaming"""

    def __init__(self, bootstrap_servers: str, topic: str = "sales_events"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.data_generator = SalesDataGenerator()

        # Configure Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas to acknowledge
            retries=3,
            retry_backoff_ms=100,
            batch_size=16384,
            linger_ms=10,  # Small delay to allow batching
            buffer_memory=33554432,
            compression_type="gzip",  # Compress messages (gzip is built-in)
            max_in_flight_requests_per_connection=1,  # Ensure ordering
        )

        logger.info(f"Kafka producer initialized with brokers: {bootstrap_servers}")
        logger.info(f"Target topic: {topic}")

    def produce_sales_event(self) -> Dict:
        """Produce a single sales event to Kafka"""
        event = self.data_generator.generate_sales_event()

        try:
            # Use order_id as key for partitioning
            future = self.producer.send(self.topic, key=event["order_id"], value=event)

            # Get metadata about the sent message (optional)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Sent message to topic {record_metadata.topic}, "
                f"partition {record_metadata.partition}, "
                f"offset {record_metadata.offset}"
            )

            return event

        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            raise

    def produce_continuous(
        self,
        count: Optional[int] = None,
        interval: float = 1.0,
        burst_mode: bool = False,
    ) -> None:
        """
        Produce sales events continuously

        Args:
            count: Number of events to produce (None for infinite)
            interval: Seconds between events (ignored in burst_mode)
            burst_mode: Send events as fast as possible
        """
        events_sent = 0
        start_time = time.time()

        try:
            while count is None or events_sent < count:
                event = self.produce_sales_event()
                events_sent += 1

                if events_sent % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed
                    logger.info(f"Sent {events_sent} events (avg rate: {rate:.2f}/sec)")

                if not burst_mode and interval > 0:
                    time.sleep(interval)

                # Occasional status update
                if events_sent % 10 == 0:
                    logger.debug(
                        f"Latest event: Order {event['order_id']}, "
                        f"Product: {event['product_name']}, "
                        f"Sales: ${event['sales']}"
                    )

        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping producer...")

        finally:
            elapsed = time.time() - start_time
            rate = events_sent / elapsed if elapsed > 0 else 0
            logger.info(f"Producer stopped. Total events sent: {events_sent}")
            logger.info(
                f"Total time: {elapsed:.2f}s, Average rate: {rate:.2f} events/sec"
            )

            # Ensure all messages are sent
            self.producer.flush()
            self.producer.close()


def load_config(config_path: str) -> Dict:
    """Load configuration from JSON file"""
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Config file {config_path} not found, using defaults")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Kafka Sales Data Producer")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument(
        "--brokers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (comma-separated)",
    )
    parser.add_argument(
        "--topic", type=str, default="sales_events", help="Kafka topic name"
    )
    parser.add_argument(
        "--count", type=int, help="Number of events to produce (default: infinite)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between events (default: 1.0)",
    )
    parser.add_argument(
        "--burst", action="store_true", help="Send events as fast as possible"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration if provided
    config = {}
    if args.config:
        config = load_config(args.config)

    # Use config values as defaults, then override with CLI args
    bootstrap_servers = args.brokers or config.get(
        "bootstrap_servers", "localhost:9092"
    )
    topic = args.topic or config.get("topic", "sales_events")
    count = args.count or config.get("count")
    interval = args.interval if args.interval != 1.0 else config.get("interval", 1.0)
    burst_mode = args.burst or config.get("burst_mode", False)

    logger.info("Starting Kafka producer...")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Count: {count or 'infinite'}")
    logger.info(f"Interval: {interval}s")
    logger.info(f"Burst mode: {burst_mode}")

    try:
        producer = KafkaSalesProducer(bootstrap_servers, topic)
        producer.produce_continuous(count, interval, burst_mode)
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
