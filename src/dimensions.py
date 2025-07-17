# src/dimensions.py

import sqlite3
import pandas as pd
from config import DB_PATH, OUTPUT_DIR

# Ensure OUTPUT_DIR exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1) Load cleaned sales data from SQLite
conn = sqlite3.connect(str(DB_PATH))
sales_df = pd.read_sql_query(
    "SELECT * FROM sales_daily", conn, parse_dates=["order_date"]
)
conn.close()

# 2) Build dim_date table
dim_date = sales_df[["order_date"]].drop_duplicates().reset_index(drop=True)
dim_date["day"] = dim_date["order_date"].dt.day
dim_date["month"] = dim_date["order_date"].dt.month
dim_date["quarter"] = dim_date["order_date"].dt.quarter
dim_date["year"] = dim_date["order_date"].dt.year

# 3) Build dim_product table
dim_product = (
    sales_df[["product_id", "category"]].drop_duplicates().reset_index(drop=True)
)

# 4) Build fact_sales table
fact_sales = (
    sales_df.merge(dim_date, on="order_date", how="left")
    .merge(dim_product, on=["product_id", "category"], how="left")
    .loc[
        :,
        [
            "order_date",
            "product_id",
            "quantity",
            "total_sales",
            "profit",
            "unit_price",
            "profit_margin",
            "day",
            "month",
            "quarter",
            "year",
        ],
    ]
)

# 5) Write each table to Parquet
for name, df in [
    ("dim_date", dim_date),
    ("dim_product", dim_product),
    ("fact_sales", fact_sales),
]:
    file_path = OUTPUT_DIR / f"{name}.parquet"
    df.to_parquet(file_path, index=False)
    print(f"Wrote {file_path} with {len(df)} rows")
