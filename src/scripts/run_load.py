#!/usr/bin/env python3
import sys
from pathlib import Path

# Ensure src/ is on PYTHONPATH for etl imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
import pandas as pd
from etl.load_funcs import (
    get_rds_conn,
    truncate_tables,
    load_dim_date,
    load_dim_product,
    load_fact_sales,
)
from config import OUTPUT_DIR


def main():
    load_dotenv()
    conn = get_rds_conn()
    truncate_tables(conn)

    # load dims
    df_d = pd.read_parquet(OUTPUT_DIR / "dim_date.parquet")
    load_dim_date(df_d, conn)

    df_p = pd.read_parquet(OUTPUT_DIR / "dim_product.parquet")
    load_dim_product(df_p, conn)

    # load fact
    df_f = pd.read_parquet(OUTPUT_DIR / "fact_sales.parquet")
    load_fact_sales(df_f, conn)
    conn.close()
    print("[load] Data loaded into RDS")


if __name__ == "__main__":
    main()
