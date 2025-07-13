#!/usr/bin/env bash
set -e

# Change into the repo root (one level above this script)
cd "$(dirname "$0")/.."

# 1) Extract & parse dates
python src/extract.py

# 2) Load (internally runs all transforms)
python src/load.py

echo "Pipeline complete"