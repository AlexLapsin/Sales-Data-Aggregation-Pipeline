# src/load.py

import sqlite3
import pandas as pd
from extract import get_data_files, load_region_csv, validate_rows

def load_to_sqlite(db_path: str = "pipeline.db"):
    """
    Rebuilds the sales_daily table from scratch each run:
      1) Drop the table if it exists
      2) Loop over each region file and append its cleaned rows
    """
    conn = sqlite3.connect(db_path)

    # Clear out any old data
    conn.execute("DROP TABLE IF EXISTS sales_daily")

    # Recreate by appending each file
    for fpath in get_data_files():
        df    = load_region_csv(fpath)
        clean = validate_rows(df)
        clean['unit_price'] = clean['Sales'] / clean['Quantity']
        clean = clean.rename(columns={
            'Region':      'region',
            'Country':     'country',
            'Order ID':    'order_id',
            'Order Date':  'order_date',
            'Ship Date':   'ship_date',
            'Customer ID': 'customer_id',
            'Product ID':  'product_id',
            'Category':    'category',
            'Quantity':    'quantity',
            'Sales':       'total_sales',
            'Profit':      'profit'
        })

        clean.to_sql(
            'sales_daily',
            conn,
            if_exists='append',
            index=False
        )
        print(f"Appended {len(clean)} rows from {fpath}")

    conn.close()
    print(" Load complete — data in pipeline.db → sales_daily")

if __name__ == "__main__":
    load_to_sqlite()

