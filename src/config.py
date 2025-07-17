# src/config.py
from pathlib import Path

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data folders
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Database path: store in a dedicated db/ folder
DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "pipeline.db"

# Business thresholds
SALES_THRESHOLD = 10_000
