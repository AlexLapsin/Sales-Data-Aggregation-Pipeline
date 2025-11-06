#!/usr/bin/env python3
"""
One-Time Data Corruption Script

Creates a realistically flawed dataset from clean Kaggle CSV to showcase
data validation and transformation capabilities in the Silver layer.

Usage:
    python tools/create_corrupted_dataset.py

Input:  data/raw/Global_Superstore_latest.csv (clean Kaggle data)
Output: data/raw/Global_Superstore_corrupted.csv (with 5% intentional flaws)

Corruptions Added (Total ~2,550 rows / 5%):
- Duplicates: 1,500 rows (2.9%)
- NULL customer_name: 250 rows (0.5%)
- NULL postal_code: 250 rows (0.5%)
- Invalid sales: 250 rows (150 zero, 100 negative)
- Date violations: 150 rows (ship_date < order_date)
- Outlier sales: 100 rows (> SALES_THRESHOLD)
- Invalid quantity: 50 rows (zero or negative)
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment
load_dotenv()
SALES_THRESHOLD = float(os.getenv("SALES_THRESHOLD", "10000"))

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "Global_Superstore_latest.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "Global_Superstore_corrupted.csv"


def main():
    print("=" * 80)
    print("DATA CORRUPTION SCRIPT - One-Time Execution")
    print("=" * 80)

    # Load clean data
    print(f"\n[1/8] Loading clean Kaggle dataset...")
    print(f"      Input: {INPUT_FILE}")

    # Try different encodings
    encodings = ["utf-8", "latin-1", "iso-8859-1", "cp1252"]
    df = None
    for encoding in encodings:
        try:
            df = pd.read_csv(INPUT_FILE, encoding=encoding)
            print(f"      Successfully loaded with {encoding} encoding")
            break
        except UnicodeDecodeError:
            continue

    if df is None:
        raise ValueError(f"Could not read CSV with any standard encoding")

    original_rows = len(df)
    print(f"      Loaded: {original_rows:,} rows")

    # Track corrupted indices to avoid overlap
    corrupted_indices = set()

    # 1. Add Duplicates (1,500 rows)
    print(f"\n[2/8] Adding duplicates...")
    duplicate_count = 1500
    sample_indices = np.random.choice(df.index, size=duplicate_count, replace=False)
    duplicates = df.loc[sample_indices].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    corrupted_indices.update(sample_indices)
    print(f"      Added: {duplicate_count:,} duplicate rows")

    # 2. NULL customer_name (250 rows)
    print(f"\n[3/8] Adding NULL customer names...")
    null_customer_count = 250
    available = [
        i for i in df.index if i not in corrupted_indices and i < original_rows
    ]
    null_customer_indices = np.random.choice(
        available, size=min(null_customer_count, len(available)), replace=False
    )
    df.loc[null_customer_indices, "Customer Name"] = None
    corrupted_indices.update(null_customer_indices)
    print(f"      Set {len(null_customer_indices)} customer names to NULL")

    # 3. NULL postal_code (250 rows)
    print(f"\n[4/8] Adding NULL postal codes...")
    null_postal_count = 250
    available = [
        i for i in df.index if i not in corrupted_indices and i < original_rows
    ]
    null_postal_indices = np.random.choice(
        available, size=min(null_postal_count, len(available)), replace=False
    )
    df.loc[null_postal_indices, "Postal Code"] = None
    corrupted_indices.update(null_postal_indices)
    print(f"      Set {len(null_postal_indices)} postal codes to NULL")

    # 4. Invalid sales (250 rows: 150 zero, 100 negative)
    print(f"\n[5/8] Adding invalid sales values...")
    available = [
        i for i in df.index if i not in corrupted_indices and i < original_rows
    ]

    # Zero sales (150)
    zero_sales_indices = np.random.choice(
        available[: len(available) // 2],
        size=min(150, len(available) // 2),
        replace=False,
    )
    df.loc[zero_sales_indices, "Sales"] = 0
    corrupted_indices.update(zero_sales_indices)

    # Negative sales (100)
    available = [i for i in available if i not in zero_sales_indices]
    neg_sales_indices = np.random.choice(
        available, size=min(100, len(available)), replace=False
    )
    df.loc[neg_sales_indices, "Sales"] = df.loc[neg_sales_indices, "Sales"] * -1
    corrupted_indices.update(neg_sales_indices)
    print(f"      Set 150 sales to zero, 100 to negative")

    # 5. Date violations (150 rows: ship_date < order_date)
    print(f"\n[6/8] Adding date logic violations...")
    available = [
        i for i in df.index if i not in corrupted_indices and i < original_rows
    ]
    date_violation_indices = np.random.choice(
        available, size=min(150, len(available)), replace=False
    )

    for idx in date_violation_indices:
        # Make ship date before order date
        order_date = pd.to_datetime(df.loc[idx, "Order Date"])
        invalid_ship_date = order_date - timedelta(days=np.random.randint(1, 30))
        df.loc[idx, "Ship Date"] = invalid_ship_date.strftime("%m/%d/%Y")

    corrupted_indices.update(date_violation_indices)
    print(
        f"      Created {len(date_violation_indices)} date violations (ship_date < order_date)"
    )

    # 6. Outlier sales (100 rows: > SALES_THRESHOLD)
    print(f"\n[7/8] Adding sales outliers...")
    available = [
        i for i in df.index if i not in corrupted_indices and i < original_rows
    ]
    outlier_indices = np.random.choice(
        available, size=min(100, len(available)), replace=False
    )
    df.loc[outlier_indices, "Sales"] = np.random.uniform(
        SALES_THRESHOLD + 1, SALES_THRESHOLD * 2, size=len(outlier_indices)
    )
    corrupted_indices.update(outlier_indices)
    print(
        f"      Set {len(outlier_indices)} sales values above ${SALES_THRESHOLD:,.0f} threshold"
    )

    # 7. Invalid quantity (50 rows: zero or negative)
    print(f"\n[8/8] Adding invalid quantities...")
    available = [
        i for i in df.index if i not in corrupted_indices and i < original_rows
    ]
    invalid_qty_indices = np.random.choice(
        available, size=min(50, len(available)), replace=False
    )
    df.loc[invalid_qty_indices, "Quantity"] = np.random.choice(
        [0, -1, -2, -3], size=len(invalid_qty_indices)
    )
    corrupted_indices.update(invalid_qty_indices)
    print(f"      Set {len(invalid_qty_indices)} quantities to zero or negative")

    # Save corrupted dataset
    print(f"\n[SAVING] Writing corrupted dataset...")
    df.to_csv(OUTPUT_FILE, index=False)

    # Summary
    print("\n" + "=" * 80)
    print("CORRUPTION SUMMARY")
    print("=" * 80)
    print(f"Original rows:     {original_rows:,}")
    print(f"Duplicates added:  {duplicate_count:,}")
    print(f"Total rows:        {len(df):,}")
    print(
        f"Corrupted rows:    {len(corrupted_indices):,} ({len(corrupted_indices)/original_rows*100:.1f}%)"
    )
    print(f"\nCorruption breakdown:")
    print(f"  - Duplicates:        1,500 rows (2.9%)")
    print(f"  - NULL customer:     {len(null_customer_indices):,} rows")
    print(f"  - NULL postal code:  {len(null_postal_indices):,} rows")
    print(f"  - Zero sales:        150 rows")
    print(f"  - Negative sales:    100 rows")
    print(f"  - Date violations:   {len(date_violation_indices):,} rows")
    print(f"  - Sales outliers:    {len(outlier_indices):,} rows")
    print(f"  - Invalid quantity:  {len(invalid_qty_indices):,} rows")
    print(f"\nOutput file: {OUTPUT_FILE}")
    print("\n" + "=" * 80)
    print("SUCCESS - Dataset corrupted successfully!")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Delete regional CSV files: rm data/raw/*_orders.csv")
    print("2. Clean S3 Bronze: aws s3 rm s3://raw-bucket/bronze/ --recursive")
    print("3. Clean S3 Silver: aws s3 rm s3://processed-bucket/silver/ --recursive")
    print("4. Upload corrupted CSV to Bronze")
    print("5. Run Spark transformation with validation rules")


if __name__ == "__main__":
    main()
