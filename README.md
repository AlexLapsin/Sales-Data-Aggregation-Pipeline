# Sales Data Aggregation Pipeline

## Project Layout

- `src/etl/` : reusable ETL functions
- `src/scripts/`: orchestration entry points
- `config.py` : paths & thresholds
- `.env` : secure credentials (not checked in)
- `requirements.txt`: pinned dependencies
- `tests/`: unit tests (coming soon)

## Setup

1. Clone repo
2. `cp .env.example .env` and fill in values
3. `pip install -r requirements.txt`

## Run the pipeline

```bash
python src/scripts/create_tables.py   # ensure schema
python src/scripts/run_extract.py     # fetch & stage raw data
python src/scripts/run_transform.py   # build parquet star-schema
python src/scripts/run_load.py        # bulk load into RDS
