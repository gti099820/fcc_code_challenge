# src/config.py
from pathlib import Path

# --- Project Root ---
BASE_DIR = Path(__file__).resolve().parent.parent

# --- Data Directories ---
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
DIM_DIR = PROCESSED_DATA_DIR / "dimensions"
FACT_DIR = PROCESSED_DATA_DIR / "facts"

# --- Source File Mappings ---
# Maps a simple name to its corresponding CSV file
SOURCE_FILES = {
    "channel": "channel_code.csv",
    "plan": "plan.csv",
    "payment_frequency": "plan_payment_frequency.csv",
    "status": "status_code.csv",
    "user": "user.csv",
    "payment_detail": "user_payment_detail.csv",
    "user_plan": "user_plan.csv",
    "play_session": "user_play_session.csv",
    "registration": "user_registration.csv",
}

# --- Date Dimension Settings ---
DATE_DIM_START = "2024-01-01"
DATE_DIM_END = "2025-12-31" # Forecasting for 2025

# --- Output Files ---
OUTPUT_FORMAT = "parquet" # Use 'csv' or 'parquet' [cite: 11]