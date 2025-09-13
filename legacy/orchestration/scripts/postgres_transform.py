#!/usr/bin/env python3
# ========================================
# ⚠️  DEPRECATION WARNING  ⚠️
# ========================================
# This script is DEPRECATED and will be removed in Q3 2025.
# Please migrate to the modern cloud-native pipeline.
# See legacy/README.md for migration guide.
# ========================================

import warnings

warnings.warn(
    "Legacy PostgreSQL scripts are deprecated. "
    "Please migrate to the modern cloud-native pipeline. "
    "See legacy/README.md for migration guide.",
    DeprecationWarning,
    stacklevel=2,
)

from dotenv import load_dotenv
import os
import logging
import pandas as pd
from legacy.src.etl.extract import get_data_files, load_region_csv
from legacy.src.etl.transform import (
    parse_dates,
    clean_basic,
    cap_extremes,
    derive_fields,
    rename_columns,
    df_to_s3_parquet,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    load_dotenv()

    # Destination: processed bucket
    processed_bucket = os.getenv("PROCESSED_BUCKET")
    if not processed_bucket:
        raise RuntimeError("PROCESSED_BUCKET is not set")

    prefix = os.getenv("S3_PREFIX", "")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # Source: raw files in S3
    keys = get_data_files()
    if not keys:
        raise RuntimeError(
            "No *_orders.csv files found in RAW bucket (S3_BUCKET/S3_PREFIX)."
        )

    dfs = [load_region_csv(k) for k in keys]
    df = pd.concat(dfs, ignore_index=True)

    # Transform pipeline
    cleaned = (
        df.pipe(parse_dates)
        .pipe(clean_basic)
        .pipe(cap_extremes)
        .pipe(derive_fields)
        .pipe(rename_columns)
    )

    # Build star schema
    dim_date = (
        cleaned[["order_date"]]
        .drop_duplicates()
        .assign(
            day=lambda d: d.order_date.dt.day,
            month=lambda d: d.order_date.dt.month,
            quarter=lambda d: d.order_date.dt.quarter,
            year=lambda d: d.order_date.dt.year,
        )
    )
    df_to_s3_parquet(dim_date, processed_bucket, f"{prefix}dim_date.parquet")

    dim_prod = cleaned[["product_id", "category"]].drop_duplicates()
    df_to_s3_parquet(dim_prod, processed_bucket, f"{prefix}dim_product.parquet")

    fact_cols = [
        "order_date",
        "product_id",
        "quantity",
        "total_sales",
        "profit",
        "unit_price",
        "profit_margin",
    ]
    fact = cleaned[fact_cols]
    df_to_s3_parquet(fact, processed_bucket, f"{prefix}fact_sales.parquet")

    logger.info(
        "[transform] Uploaded dim_date(%d), dim_product(%d), fact_sales(%d) to s3://%s/%s",
        len(dim_date),
        len(dim_prod),
        len(fact),
        processed_bucket,
        prefix,
    )


if __name__ == "__main__":
    main()
