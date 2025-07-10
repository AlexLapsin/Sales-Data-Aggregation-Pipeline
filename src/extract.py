# src/extract.py

import glob
import pandas as pd

def load_region_csv(path: str) -> pd.DataFrame:
    """
    Load one region’s CSV into a DataFrame,
    parsing Order Date and Ship Date as datetime.
    """
    df = pd.read_csv(
        path,
        parse_dates=['Order Date', 'Ship Date'], 
        dayfirst=True  # adjust if your dates are D/M/Y
    )
    return df

def validate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows where Quantity > 0 and Sales > 0.
    """
    filtered = df[(df['Quantity'] > 0) & (df['Sales'] > 0)].copy()
    return filtered

def main():
    files = glob.glob('data/*_orders.csv')
    if not files:
        print("No region files found in data/. Did you run the splitter?")
        return

    total_loaded = 0
    total_valid = 0

    for fpath in files:
        df = load_region_csv(fpath)
        clean = validate_rows(df)

        n_loaded = len(df)
        n_valid = len(clean)
        total_loaded += n_loaded
        total_valid += n_valid

        print(f"{fpath:30} → loaded: {n_loaded:6} rows   valid: {n_valid:6} rows")

    print("-" * 60)
    print(f"Processed {len(files)} files")
    print(f"Total rows loaded: {total_loaded}")
    print(f"Total rows valid:  {total_valid}")

if __name__ == "__main__":
    main()
