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
from io import StringIO
import pandas as pd
from legacy.src.etl.load import (
    s3_parquet_to_df,
    get_rds_conn,
)  # make sure s3_parquet_to_df is in load_funcs.py

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def _copy_df(cur, table: str, cols: list[str], df: pd.DataFrame) -> None:
    """COPY a DataFrame to Postgres using CSV over STDIN (with header)."""
    buf = StringIO()
    # Dates as ISO, empty strings for NaN -> NULL on COPY
    df.to_csv(buf, index=False, header=True, date_format="%Y-%m-%d")
    buf.seek(0)
    cols_sql = ", ".join(cols)
    sql = f"COPY {table} ({cols_sql}) FROM STDIN WITH CSV HEADER"
    cur.copy_expert(sql, buf)


def main():
    load_dotenv()

    processed_bucket = os.getenv("PROCESSED_BUCKET")
    if not processed_bucket:
        raise RuntimeError("PROCESSED_BUCKET is not set")

    prefix = os.getenv("S3_PREFIX", "") or ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # --- Read processed Parquet from S3 (in-memory) ---
    logger.info("Reading processed Parquet from s3://%s/%s*", processed_bucket, prefix)
    df_dim_date = s3_parquet_to_df(processed_bucket, f"{prefix}dim_date.parquet")
    df_dim_prod = s3_parquet_to_df(processed_bucket, f"{prefix}dim_product.parquet")
    df_fact = s3_parquet_to_df(processed_bucket, f"{prefix}fact_sales.parquet")

    # Normalize dtypes for COPY
    df_dim_date["order_date"] = pd.to_datetime(df_dim_date["order_date"]).dt.date
    df_dim_prod["product_id"] = df_dim_prod["product_id"].astype(str)
    df_fact["order_date"] = pd.to_datetime(df_fact["order_date"]).dt.date
    df_fact["product_id"] = df_fact["product_id"].astype(str)

    # --- Connect & load with COPY ---
    conn = get_rds_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # 1) Idempotent reset (fast)
            logger.info("Truncating target tables (restart identity).")
            cur.execute("TRUNCATE fact_sales RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE dim_product RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE dim_date RESTART IDENTITY CASCADE;")

            # 2) COPY dimensions directly into targets (exclude PKs to auto-generate)
            logger.info("COPY -> dim_date")
            _copy_df(
                cur,
                "dim_date",
                ["order_date", "day", "month", "quarter", "year"],
                df_dim_date[["order_date", "day", "month", "quarter", "year"]],
            )

            logger.info("COPY -> dim_product")
            _copy_df(
                cur,
                "dim_product",
                ["product_id", "category"],
                df_dim_prod[["product_id", "category"]],
            )

            # 3) COPY fact rows into a temp staging table with natural keys
            logger.info("COPY -> stage_fact_sales")
            cur.execute(
                """
                CREATE TEMP TABLE stage_fact_sales (
                    order_date     date,
                    product_id     text,
                    quantity       int,
                    total_sales    numeric,
                    profit         numeric,
                    unit_price     numeric,
                    profit_margin  numeric
                ) ON COMMIT DROP;
            """
            )
            _copy_df(
                cur,
                "stage_fact_sales",
                [
                    "order_date",
                    "product_id",
                    "quantity",
                    "total_sales",
                    "profit",
                    "unit_price",
                    "profit_margin",
                ],
                df_fact[
                    [
                        "order_date",
                        "product_id",
                        "quantity",
                        "total_sales",
                        "profit",
                        "unit_price",
                        "profit_margin",
                    ]
                ],
            )

            # 4) Resolve FKs and insert into final fact table in one SQL
            logger.info("Inserting into fact_sales (resolving FKs).")
            cur.execute(
                """
                INSERT INTO fact_sales (
                    date_id, product_sk, quantity, total_sales, profit, unit_price, profit_margin
                )
                SELECT
                    d.date_id,
                    p.product_sk,
                    s.quantity,
                    s.total_sales,
                    s.profit,
                    s.unit_price,
                    s.profit_margin
                FROM stage_fact_sales s
                JOIN dim_date d    ON d.order_date = s.order_date
                JOIN dim_product p ON p.product_id = s.product_id;
            """
            )

        conn.commit()
        logger.info(
            "Load complete: dim_date(%d), dim_product(%d), fact_sales(%d).",
            len(df_dim_date),
            len(df_dim_prod),
            len(df_fact),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
