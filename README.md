# Sales Data Aggregation Pipeline

> **Temporary README – full documentation coming soon!**

---

## 🚀 Quickstart

### 🔹 With Docker Compose

```bash
docker compose up --build etl
```

### 🔹 Without Docker

```bash
# 1. Create & activate virtualenv
python -m venv .venv
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python src/extract.py
python src/transform.py
python src/load.py
python src/dimensions.py
```

---

## 📂 Project Structure

* `db/` — SQLite database file(s)
* `data/` — Raw CSVs by region
* `output/` — Parquet exports of dims & facts
* `src/` — ETL & schema-building scripts

  * `config.py`
  * `logging_config.py`
  * `extract.py`
  * `transform.py`
  * `load.py`
  * `dimensions.py`
* `docker-compose.yml` — Local multi-container orchestration
* `Dockerfile` — Container image recipe
* `requirements.txt` — Python dependencies
* `README.md` — This file

---

## 📋 Features

* **Extract:** Ingests regional CSVs from `data/`
* **Transform:** Cleans data, caps outliers, derives fields (`unit_price`, `profit_margin`)
* **Load:** Appends to SQLite at `db/pipeline.db`
* **Schema:** Builds `dim_date`, `dim_product`, `fact_sales`
* **Export:** Writes Parquet files to `output/`

---

## 🔧 Configuration

### `src/config.py`

```python
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "pipeline.db"
SALES_THRESHOLD = 10_000
```

---

## 📈 Next Steps

1. Add CI badges & coverage reports
2. Flesh out detailed docs & examples
3. Integrate Airflow for scheduling
4. Enhance logging & monitoring

---
