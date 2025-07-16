import glob
import pandas as pd
from config import DATA_DIR


def get_data_files():
    """Return sorted list of region CSV paths under data/"""
    return sorted(glob.glob(f"{DATA_DIR}/*_orders.csv"))


def load_region_csv(path: str) -> pd.DataFrame:
    """
    Load one region’s CSV into raw strings (no parsing).
    """
    return pd.read_csv(path)


if __name__ == "__main__":
    files = get_data_files()
    print(f"Found {len(files)} files: {files}")
    df_sample = load_region_csv(files[0])
    print("Sample raw data columns:", df_sample.columns.tolist())
