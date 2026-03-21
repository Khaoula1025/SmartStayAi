import json
import os
import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger('smartstay.init_db')

# Project root: two levels up from backend/app/db/
PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent.parent
PREDICTION_CSV = PROJECT_ROOT / 'data' / 'prediction' / 'predictions_2026.csv'
METRICS_JSON   = PROJECT_ROOT / 'data' / 'prediction' / 'model_metrics.json'
HOTEL          = 'hickstead'


def init_db():
    """
    Create all tables and seed predictions from CSV if empty.
    Called once from main.py on startup.
    """
    # Import all models here so Base knows about them before create_all
    from app.db.session import Base, engine
    from app.models import prediction, actual, rate_decision, pipeline_run  # noqa

    Base.metadata.create_all(bind=engine)
    log.info("Tables verified / created")

    from app.db.session import SessionLocal
    db = SessionLocal()
    try:
        _seed_predictions(db)
    finally:
        db.close()


def _seed_predictions(db):
    from app.models.prediction import ModelRun, Prediction

    if db.query(Prediction).count() > 0:
        log.info("Predictions already loaded — skipping seed")
        return

    if not PREDICTION_CSV.exists():
        log.warning(f"CSV not found at {PREDICTION_CSV} — skipping seed")
        return

    log.info(f"Seeding from {PREDICTION_CSV.name} ...")

    metrics = {}
    if METRICS_JSON.exists():
        with open(METRICS_JSON) as f:
            metrics = json.load(f)

    run = ModelRun(
        hotel             = HOTEL,
        n_training_rows   = metrics.get('n_training_rows', 638),
        n_prediction_rows = metrics.get('n_prediction_rows', 307),
        mae_operational   = metrics.get('cv_mae_operational'),
        mae_all_folds     = metrics.get('cv_mae_all_folds'),
        r2_mean           = metrics.get('cv_r2_mean'),
        occ_accuracy_pct  = metrics.get('occ_accuracy_pct'),
        features          = metrics.get('features', []),
        model_type        = metrics.get('model', 'GBM+RF ensemble 60/40'),
        stage2_regime     = metrics.get('stage2_regime', 'moderate'),
        promoted          = True,
    )
    db.add(run)
    db.flush()

    df    = pd.read_csv(PREDICTION_CSV, parse_dates=['date'])
    count = 0

    for _, row in df.iterrows():
        def sf(col):
            v = row.get(col)
            return float(v) if v is not None and pd.notna(v) else None
        def si(col):
            v = row.get(col)
            return int(v) if v is not None and pd.notna(v) else None

        db.add(Prediction(
            hotel            = HOTEL,
            run_id           = run.run_id,
            date             = row['date'].date(),
            day_of_week      = row.get('day_of_week'),
            month            = si('month'),
            days_ahead       = si('days_ahead'),
            stage1_occ       = sf('stage1_occ'),
            pace_adj         = sf('pace_adj'),
            bob_adj          = sf('bob_adj'),
            predicted_occ    = sf('predicted_occ'),
            predicted_rooms  = si('predicted_rooms'),
            occ_low          = sf('occ_low'),
            occ_high         = sf('occ_high'),
            recommended_rate = sf('recommended_rate'),
            rate_tier        = row.get('rate_tier'),
            floor_price      = sf('floor_price'),
            bob_occ          = sf('bob_occ'),
            pace_gap         = si('pace_gap'),
            pickup_velocity  = sf('pickup_velocity'),
            is_bank_holiday  = si('is_bank_holiday'),
            is_local_event   = si('is_local_event'),
            data_quality     = row.get('data_quality'),
        ))
        count += 1

    db.commit()
    log.info(f"  Seeded {count} predictions")