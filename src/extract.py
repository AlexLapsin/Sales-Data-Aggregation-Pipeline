import glob
import pandas as pd
from config import DATA_DIR

def get_data_files():
    """
    Return a list of all region CSV paths under data/ in the CWD.
    """
    return sorted(glob.glob('DATA_DIR/*_orders.csv'))

def load_region_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Parse month-day-year exactly, coerce bad rows to NaT
    df['Order Date'] = pd.to_datetime(
        df['Order Date'],
        format='%m-%d-%Y',
        errors='coerce'
    )
    df['Ship Date']  = pd.to_datetime(
        df['Ship Date'],
        format='%m-%d-%Y',
        errors='coerce'
    )

    return df

def validate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows where Quantity > 0 and Sales > 0,
    using DataFrame.query() for readability.
    """
    return df.query('Quantity > 0 and Sales > 0').copy()

def main():
    files = get_data_files()
    if not files:
        print("No region files found in data/. Make sure you’re in the project root.")
        return

    total_loaded = total_valid = 0

    for fpath in files:
        df    = load_region_csv(fpath)
        clean = validate_rows(df)

        n_loaded, n_valid = len(df), len(clean)
        total_loaded     += n_loaded
        total_valid      += n_valid

        print(f"{fpath:30} → loaded: {n_loaded:6}   valid: {n_valid:6}")

    print("-" * 60)
    print(f"Processed {len(files)} files")
    print(f"Total rows loaded: {total_loaded}")
    print(f"Total rows valid:  {total_valid}")

if __name__ == "__main__":
    main()