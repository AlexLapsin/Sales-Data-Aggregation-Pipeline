import sqlite3
import pandas as pd

from extract import get_data_files, load_region_csv
from transform import clean_basic, cap_extremes, derive_fields

def load_to_sqlite(db_path: str = "pipeline.db"):

    print("file                     | total   | post-clean")
    print("----------------------------------------------")
    for path in get_data_files():
        df = pd.read_csv(path)
        total = len(df)
        # mimic clean_basic
        post = df.query(
            'Quantity > 0 and Sales > 0 '
            'and `Order Date`.notnull() '
            'and `Ship Date`.notnull()'
        ).shape[0]
        print(f"{path:25} | {total:7} | {post:10}")

    
    """
    Rebuilds the sales_daily table from scratch each run:
      1) Drop the table if it exists
      2) Loop over each region file and append its cleaned rows
    """
    conn = sqlite3.connect(db_path)

    # 1) Clear out any old data
    conn.execute("DROP TABLE IF EXISTS sales_daily")

    # 2) Recreate by appending each file
    for fpath in get_data_files():
        # a) Extract
        raw = load_region_csv(fpath)

        # b) Transform
        clean = clean_basic(raw)
        clean = cap_extremes(clean)
        clean = derive_fields(clean)

        # c) Rename to match your SQL schema
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

        # d) Load
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
