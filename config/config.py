"""
config.py — SmartStay Intelligence
====================================
Single source of truth for all paths, DB settings, and constants.
Imported by every script and by the Airflow DAG.

Override any value via environment variables or a .env file
in the project root.

Project layout expected:
  smartstay-intelligence/
    config.py
    scripts/
      01_clean_occupancy.py
      ...
      08_pipeline.py
      09_airflow_dag.py
    data/
      raw/          ← raw Excel uploads from PMS / pickup tool
      processed/    ← cleaned CSVs (output of scripts 01-06)
      prediction/   ← matrix + model outputs (scripts 06-07b)
      models/       ← saved model files (.joblib)
    logs/
    tests/
"""

import os
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
# config.py lives in config/, so .parent gives config/ and .parent.parent gives
# the actual project root (smartstay-intelligence/).
ROOT = Path(__file__).resolve().parent.parent

# ── Data directories ──────────────────────────────────────────────────────────
RAW_DIR        = Path(os.getenv('RAW_DIR',        ROOT / 'data' / 'raw'))
PROCESSED_DIR  = Path(os.getenv('PROCESSED_DIR',  ROOT / 'data' / 'processed'))
PREDICTION_DIR = Path(os.getenv('PREDICTION_DIR', ROOT / 'data' / 'prediction'))
MODELS_DIR     = Path(os.getenv('MODELS_DIR',     ROOT / 'data' / 'models'))
LOGS_DIR       = Path(os.getenv('LOGS_DIR',       ROOT / 'logs'))
SCRIPTS_DIR    = Path(os.getenv('SCRIPTS_DIR',    ROOT / 'scripts'))

# ── Raw input files ───────────────────────────────────────────────────────────
# Scripts 01-05 read from these paths.
# Replace filenames here when new exports arrive from the PMS.
RAW_FILES = {
    'occupancy_1':  RAW_DIR / 'UNOHICK_Occupancy_20260227_235408.xlsx',
    'occupancy_2':  RAW_DIR / 'UNOHICK_Occupancy_20260227_235324.xlsx',
    'day_by_day':   RAW_DIR / 'Day_by_day_budget_split.xlsx',
    'bookingcom':   RAW_DIR / 'the-hickstead-hotel-by-uno_bookingdotcom_lowest_los1_2guests_standard_room_only__1_.xlsx',
    'pickup_bob':   RAW_DIR / 'Uno_Hotels_Pickup__27_02_2026__1_.xlsx',
    'pickup_stly':  RAW_DIR / 'Uno_Hotels_Pickup_18_12_25.xlsx',
    'fit_rates':    RAW_DIR / 'FIT_Static_Final_Rates_2025-2026.xlsx',
    'targets':      RAW_DIR / 'Uno_Hotels_-_2025_Targets.xlsx',
}

# ── Processed CSVs (output of scripts 01-05) ──────────────────────────────────
CLEAN_FILES = {
    'occupancy':      PROCESSED_DIR / 'clean_occupancy.csv',
    'day_by_day':     PROCESSED_DIR / 'clean_day_by_day.csv',
    'bookingcom':     PROCESSED_DIR / 'clean_bookingcom.csv',
    'pickup':         PROCESSED_DIR / 'clean_pickup.csv',
    'fit_rates':      PROCESSED_DIR / 'clean_fit_rates.csv',
    'floor_by_date':  PROCESSED_DIR / 'clean_floor_by_date.csv',
}

# ── Matrix + prediction files (output of scripts 06-07b) ─────────────────────
# training_matrix and prediction_matrix are written to PROCESSED_DIR by
# 06_build_matrix.py; downstream predictions stay in PREDICTION_DIR.
MATRIX_FILES = {
    'training':         PROCESSED_DIR  / 'training_matrix.csv',
    'prediction':       PROCESSED_DIR  / 'prediction_matrix.csv',
    'predictions_2026': PREDICTION_DIR / 'predictions_2026.csv',
    'prophet_preds':    PREDICTION_DIR / 'prophet_predictions.csv',
    'model_comparison': PREDICTION_DIR / 'model_comparison.json',
    'model_metrics':    PREDICTION_DIR / 'model_metrics.json',
}

# ── Saved model files ─────────────────────────────────────────────────────────
MODEL_FILES = {
    'gbm':     MODELS_DIR / 'gbm_model.joblib',
    'rf':      MODELS_DIR / 'rf_model.joblib',
    'prophet': MODELS_DIR / 'prophet_model.joblib',
}

# ── Hotel constants ───────────────────────────────────────────────────────────
HOTEL      = 'hickstead'
TOT_ROOMS  = 52
HOTEL_NAME = 'The Hickstead Hotel by UNO'

# ── Prophet training window ───────────────────────────────────────────────────
# Excludes Apr-Aug 2024 (hotel opening period, avg occ 6-72%)
PROPHET_TRAIN_START = '2024-09-01'

# ── Database ──────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv('DB_HOST',     'localhost')
DB_PORT     = os.getenv('DB_PORT',     '5432')
DB_NAME     = os.getenv('DB_NAME',     'smartstay')
DB_USER     = os.getenv('DB_USER',     'smartstay')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_SCHEMA   = os.getenv('DB_SCHEMA',   'public')

def get_db_url() -> str:
    """SQLAlchemy-compatible connection URL."""
    return (
        f'postgresql://{DB_USER}:{DB_PASSWORD}'
        f'@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )

def get_db_connection():
    """Return a raw psycopg2 connection."""
    import psycopg2
    return psycopg2.connect(
        host=DB_HOST, port=int(DB_PORT), dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )

# ── Airflow ───────────────────────────────────────────────────────────────────
AIRFLOW_DAG_ID       = 'smartstay_hickstead_daily'
AIRFLOW_RETRAIN_ID   = 'smartstay_hickstead_retrain_weekly'
AIRFLOW_OWNER        = 'smartstay'
AIRFLOW_EMAIL_ALERTS = os.getenv('AIRFLOW_EMAIL', 'revenue@unhotels.co.uk')

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL  = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s  %(name)-20s  %(levelname)-8s  %(message)s'
LOG_DATE   = '%Y-%m-%d %H:%M:%S'

def get_logger(name: str):
    """Return a consistently configured logger."""
    import logging
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, datefmt=LOG_DATE)
    return logging.getLogger(name)

# ── Ensure directories exist on import ───────────────────────────────────────
for _dir in [RAW_DIR, PROCESSED_DIR, PREDICTION_DIR, MODELS_DIR, LOGS_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)