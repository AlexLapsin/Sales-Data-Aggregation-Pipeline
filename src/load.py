import sqlite3
from extract import get_data_files, load_region_csv
from config import DB_PATH
from transform import (
    parse_dates,
    clean_basic,
    cap_extremes,
    derive_fields,
    rename_columns,
)


def load_to_sqlite(db_path: str = DB_PATH):
    """
    1) Drop & recreate sales_daily
    2) For each region CSV:
       - extract
       - transform (parse, clean, cap, derive, rename)
       - append to table
    """
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS sales_daily")

    for fpath in get_data_files():
        raw = load_region_csv(fpath)
        parsed = parse_dates(raw)
        clean = clean_basic(parsed)
        capped = cap_extremes(clean)
        derived = derive_fields(capped)
        named = rename_columns(derived)

        named.to_sql("sales_daily", conn, if_exists="append", index=False)
        print(f"Appended {len(named):6} rows from {fpath}")

    conn.close()
    print("Load complete — data in pipeline.db → sales_daily")


if __name__ == "__main__":
    load_to_sqlite()
