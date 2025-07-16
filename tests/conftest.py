# tests/conftest.py
import sys
import os

# Add the 'src' directory to sys.path so tests can import modules directly
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_PATH)
