#!/usr/bin/env python3
import sys
from pathlib import Path

# Ensure src/ is on PYTHONPATH for etl imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
import pandas as pd
from etl.extract_funcs import get_data_files, load_region_csv
from pathlib import Path
from config import DATA_DIR


def main():
    load_dotenv()
    keys = get_data_files()
    dfs = [load_region_csv(k) for k in keys]
    raw = pd.concat(dfs, ignore_index=True)

    outdir = DATA_DIR / "raw"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "all_orders_raw.parquet"
    raw.to_parquet(path, index=False)
    print(f"[extract] Wrote {len(raw)} rows to {path}")


if __name__ == "__main__":
    main()
