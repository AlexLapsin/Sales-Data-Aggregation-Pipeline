#!/usr/bin/env python3
from dotenv import load_dotenv
import pandas as pd
from etl.transform_funcs import (
    parse_dates,
    clean_basic,
    cap_extremes,
    derive_fields,
    rename_columns,
)
from config import OUTPUT_DIR, DATA_DIR


def main():
    load_dotenv()
    raw_path = DATA_DIR / "raw" / "all_orders_raw.parquet"
    df = pd.read_parquet(raw_path)

    cleaned = (
        df.pipe(parse_dates)
        .pipe(clean_basic)
        .pipe(cap_extremes)
        .pipe(derive_fields)
        .pipe(rename_columns)
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # dim_date
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
    dim_date.to_parquet(OUTPUT_DIR / "dim_date.parquet", index=False)

    # dim_product
    dim_prod = cleaned[["product_id", "category"]].drop_duplicates()
    dim_prod.to_parquet(OUTPUT_DIR / "dim_product.parquet", index=False)

    # fact_sales
    fact_cols = [
        "order_date",
        "product_id",
        "quantity",
        "total_sales",
        "profit",
        "unit_price",
        "profit_margin",
    ]
    cleaned[fact_cols].to_parquet(OUTPUT_DIR / "fact_sales.parquet", index=False)

    print(
        f"[transform] Wrote dim_date({len(dim_date)}), dim_product({len(dim_prod)}), fact_sales({len(cleaned)})"
    )


if __name__ == "__main__":
    main()
