import sys
import os
import json
import argparse
import warnings
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import numpy as np
import joblib

warnings.filterwarnings('ignore')

# ── Project root ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import (
    RAW_DIR, PROCESSED_DIR, PREDICTION_DIR, MODELS_DIR,
    CLEAN_FILES, MATRIX_FILES, MODEL_FILES,
    HOTEL, TOT_ROOMS, get_logger, get_db_connection,
)

log = get_logger('smartstay.rescore')

# ── Constants ─────────────────────────────────────────────────────────────────
RESCORE_DAYS = 60       # default window: today → today+60
SEASON_MAP   = {'low': 0, 'shoulder': 1, 'high': 2}
DOW_MAP      = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}

FEATURES = [
    'month', 'dow', 'is_weekend', 'is_high_season',
    'cs_occ', 'cs_adr',
    'b_occ', 'b_adr',
    'floor_price',
    'is_bank_holiday', 'is_cultural_holiday', 'is_local_event',
    'season',
]


# ── 1. Find latest BOB pickup file ────────────────────────────────────────────

def find_latest_pickup_file() -> tuple[Path | None, str, int]:
    """
    Scan data/raw/ for pickup_YYYYMMDD.xlsx or pickup_YYYYMMDD.csv files.

    Returns:
        (path, quality, staleness_days)
        quality: 'high' (today), 'medium' (1-2 days old), 'low' (3+ days or missing)
    """
    today = date.today()
    candidates = []

    for pattern in ['pickup_*.xlsx', 'pickup_*.csv',
                    'Uno_Hotels_Pickup_*.xlsx', 'Uno_Hotels_Pickup_*.csv']:
        candidates.extend(RAW_DIR.glob(pattern))

    if not candidates:
        log.warning("No pickup file found in data/raw/ — will use fallback features")
        return None, 'low', 999

    # Sort by file modification time, newest first
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    latest = candidates[0]

    # Try to parse date from filename (pickup_20260312.xlsx)
    stem  = latest.stem
    parts = stem.replace('Uno_Hotels_Pickup_', '').replace('pickup_', '')
    file_date = None
    for fmt in ('%Y%m%d', '%d_%m_%Y', '%Y-%m-%d'):
        try:
            file_date = datetime.strptime(parts[:8], fmt).date()
            break
        except ValueError:
            continue

    if file_date is None:
        # Can't parse date — use mtime
        file_date = date.fromtimestamp(latest.stat().st_mtime)

    staleness = (today - file_date).days

    if staleness == 0:
        quality = 'high'
    elif staleness <= 2:
        quality = 'medium'
    else:
        quality = 'low'

    log.info(f"Latest pickup file: {latest.name}  (file date: {file_date}, "
             f"staleness: {staleness}d, quality: {quality})")
    return latest, quality, staleness


# ── 2. Parse BOB features from pickup file ───────────────────────────────────

def parse_pickup_file(path: Path) -> pd.DataFrame | None:
    """
    Read a pickup/BOB Excel or CSV and extract per-date BOB features.
    Handles both the 'Uno_Hotels_Pickup' format and a simple date/bob_occ/pace_gap CSV.

    Returns DataFrame with columns: [date, bob_sold, bob_occ, pickup_velocity, pace_gap]
    or None if parsing fails.
    """
    try:
        if path.suffix in ('.xlsx', '.xls', '.xlsm'):
            df = pd.read_excel(path, header=None)
        else:
            df = pd.read_csv(path)

        log.info(f"  Pickup file shape: {df.shape}")

        # ── Try clean_pickup.csv format first (already processed) ────────────
        if 'date' in [str(c).lower() for c in df.columns]:
            df.columns = [str(c).lower().strip() for c in df.columns]
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            df['date'] = df['date'].dt.date

            needed = ['date', 'bob_occ', 'pace_gap', 'pickup_velocity', 'bob_sold']
            available = [c for c in needed if c in df.columns]
            if 'date' in available and 'bob_occ' in available:
                log.info(f"  Parsed as clean pickup CSV: {len(df)} rows")
                return df[available]

        # ── Try Script 04 output (clean_pickup.csv) as fallback ──────────────
        clean_pickup = CLEAN_FILES.get('pickup')
        if clean_pickup and Path(clean_pickup).exists():
            log.info("  Raw file unparseable — falling back to clean_pickup.csv")
            df2 = pd.read_csv(clean_pickup, parse_dates=['date'])
            df2['date'] = df2['date'].dt.date
            return df2[['date', 'bob_occ', 'pace_gap', 'pickup_velocity', 'bob_sold']]

        log.warning("  Could not parse pickup file — BOB features unavailable")
        return None

    except Exception as e:
        log.warning(f"  Error reading pickup file: {e}")
        return None


# ── 3. Build feature matrix for rescore window ───────────────────────────────

def build_rescore_matrix(
    start_date:   date,
    end_date:     date,
    bob_df:       pd.DataFrame | None,
    bob_quality:  str,
) -> pd.DataFrame:
    """
    Build a feature matrix for [start_date, end_date] using:
      - Static calendar + competitor features from prediction_matrix.csv
      - Fresh BOB features from bob_df (or fallback to last known values)

    Returns DataFrame ready for model.predict()
    """
    # Load the annual prediction matrix (built by Script 06, static for year)
    pred_matrix = pd.read_csv(MATRIX_FILES['prediction'], parse_dates=['date'])
    pred_matrix['date'] = pred_matrix['date'].dt.date

    # Filter to rescore window
    window = pred_matrix[
        (pred_matrix['date'] >= start_date) &
        (pred_matrix['date'] <= end_date)
    ].copy()

    if len(window) == 0:
        raise ValueError(
            f"No rows in prediction matrix for {start_date} → {end_date}. "
            f"Matrix range: {pred_matrix['date'].min()} → {pred_matrix['date'].max()}"
        )

    log.info(f"  Rescore window: {len(window)} dates "
             f"({start_date} → {end_date})")

    # ── Merge fresh BOB features ──────────────────────────────────────────────
    if bob_df is not None and len(bob_df) > 0:
        bob_df['date'] = pd.to_datetime(bob_df['date']).dt.date
        window = window.merge(
            bob_df[['date', 'bob_occ', 'pace_gap', 'pickup_velocity']],
            on='date', how='left', suffixes=('_old', '')
        )
        # Use fresh BOB where available, keep old value as fallback
        for col in ['bob_occ', 'pace_gap', 'pickup_velocity']:
            fresh_col = col
            old_col   = f'{col}_old'
            if old_col in window.columns:
                window[col] = window[fresh_col].fillna(window[old_col])
                window.drop(columns=[old_col], inplace=True)

        n_matched = window['bob_occ'].notna().sum()
        log.info(f"  BOB features matched: {n_matched}/{len(window)} dates")
    else:
        log.info("  No BOB data — using prediction matrix values as fallback")

    # ── Encode categoricals (same as Script 07) ───────────────────────────────
    window['season'] = window['season'].map(SEASON_MAP).fillna(1).astype(int)
    window['dow']    = window['dow'].map(DOW_MAP).fillna(
        pd.to_datetime(window['date']).dt.dayofweek
    )

    # ── Fill any remaining nulls with safe defaults ───────────────────────────
    defaults = {
        'bob_occ':          window.get('b_occ', pd.Series(0.75)),  # budget occ as proxy
        'pace_gap':         0,
        'pickup_velocity':  0,
        'cs_occ':           0.70,
        'cs_adr':           75.0,
        'b_occ':            0.75,
        'b_adr':            73.0,
        'is_bank_holiday':  0,
        'is_cultural_holiday': 0,
        'is_local_event':   0,
    }
    for col, default in defaults.items():
        if col in window.columns:
            window[col] = window[col].fillna(default)

    # ── Verify all features present ───────────────────────────────────────────
    missing = [f for f in FEATURES if f not in window.columns]
    if missing:
        raise ValueError(f"Feature matrix missing columns: {missing}")

    return window


# ── 4. Load model ─────────────────────────────────────────────────────────────

def load_model() -> tuple:
    """
    Load GBM and RF models from data/models/.
    Returns (gbm, rf) tuple.
    Raises FileNotFoundError if models don't exist (script 07 must run first).
    """
    gbm_path = MODEL_FILES['gbm']
    rf_path  = MODEL_FILES['rf']

    if not gbm_path.exists():
        raise FileNotFoundError(
            f"GBM model not found at {gbm_path}. "
            "Run Script 07 first: uv run scripts/07_train_model.py"
        )

    gbm = joblib.load(gbm_path)
    rf  = joblib.load(rf_path) if rf_path.exists() else None

    log.info(f"  Model loaded: {gbm_path.name}")
    if rf:
        log.info(f"  RF loaded:    {rf_path.name}")

    return gbm, rf


# ── 5. Predict + Stage 2 adjustments (same logic as Script 07) ───────────────

def predict_and_adjust(
    window:       pd.DataFrame,
    gbm,
    rf,
    bob_quality:  str,
) -> pd.DataFrame:
    """
    Run Stage 1 (model prediction) + Stage 2 (BOB adjustments).
    Exact same logic as Script 07 so results are consistent.
    """
    X = window[FEATURES].astype(float)

    # Stage 1: ensemble prediction
    s1_gbm = gbm.predict(X)
    s1_rf  = rf.predict(X) if rf is not None else s1_gbm
    s1     = 0.60 * s1_gbm + 0.40 * s1_rf
    s1     = np.clip(s1, 0.0, 1.0)

    # Stage 1.5: opening period correction (same as Script 07)
    s1_corrected = s1.copy()
    for i, row in window.reset_index(drop=True).iterrows():
        d = pd.to_datetime(row['date'])
        if d.month == 4 and s1_corrected[i] < 0.45:
            stly = row.get('b_occ', 0.75)
            s1_corrected[i] = 0.40 * s1_corrected[i] + 0.60 * stly

    # Stage 2: BOB / pace adjustments
    pace_gap       = window['pace_gap'].fillna(0).values
    bob_occ        = window['bob_occ'].fillna(
                         pd.Series(s1_corrected)
                     ).values
    pickup_vel     = window['pickup_velocity'].fillna(0).values
    days_ahead     = window['days_ahead'].fillna(90).values

    bob_adj  = np.zeros(len(window))
    pace_adj = np.zeros(len(window))

    for i in range(len(window)):
        base = s1_corrected[i]

        # Pace adjustment (long-range signal)
        if days_ahead[i] > 90:
            if pace_gap[i] < -15:
                scale = min(abs(pace_gap[i]) / TOT_ROOMS, 1.0)
                pace_adj[i] = -0.08 * scale
            elif pace_gap[i] > 5:
                scale = min(pace_gap[i] / TOT_ROOMS, 1.0)
                pace_adj[i] = +0.04 * scale

        # BOB adjustment (close-in signal)
        divergence = bob_occ[i] - base
        if days_ahead[i] <= 30 and abs(divergence) > 0.15:
            bob_adj[i] = 0.35 * divergence
        elif days_ahead[i] <= 90 and abs(divergence) > 0.25:
            bob_adj[i] = 0.15 * divergence

        # Pickup velocity micro-adjustment
        if days_ahead[i] <= 60:
            if pickup_vel[i] > 3:
                bob_adj[i] += 0.03
            elif pickup_vel[i] < -2:
                bob_adj[i] -= 0.03

    total_adj = pace_adj + bob_adj
    predicted = np.clip(s1_corrected + total_adj, 0.0, 1.0)

    # CI: model disagreement + cv spread
    mean_mae    = 0.12
    model_spread = np.abs(s1_gbm - s1_rf) if rf is not None else np.zeros(len(window))
    ci_half      = np.maximum(model_spread * 1.5, mean_mae)
    occ_low      = np.clip(predicted - ci_half, 0.0, 1.0)
    occ_high     = np.clip(predicted + ci_half, 0.0, 1.0)

    return pd.DataFrame({
        'stage1_occ':      np.round(s1_corrected, 4),
        'pace_adj':        np.round(pace_adj,  4),
        'bob_adj':         np.round(bob_adj,   4),
        'predicted_occ':   np.round(predicted, 4),
        'predicted_rooms': np.round(predicted * TOT_ROOMS).astype(int),
        'occ_low':         np.round(occ_low,   4),
        'occ_high':        np.round(occ_high,  4),
    })


# ── 6. Rate recommendation (same tiers as Script 07) ─────────────────────────

def recommend_rate(row: pd.Series) -> tuple[float, str]:
    occ          = row['predicted_occ']
    floor        = row.get('floor_price', 79.0) or 79.0
    days_ahead   = row.get('days_ahead', 30) or 30
    is_event     = row.get('is_local_event', 0) or 0
    is_holiday   = row.get('is_bank_holiday', 0) or 0

    # Tier thresholds
    if occ >= 0.92:
        rate, tier = floor * 2.20, 'premium'
    elif occ >= 0.82:
        rate, tier = floor * 1.80, 'high'
    elif occ >= 0.68:
        rate, tier = floor * 1.40, 'standard'
    elif occ >= 0.50:
        rate, tier = floor * 1.10, 'value'
    else:
        rate, tier = floor * 0.90, 'promotional'

    # Event uplift
    if is_event or is_holiday:
        rate *= 1.10

    # Close-in premium (< 7 days, high occ)
    if days_ahead <= 7 and occ >= 0.75:
        rate *= 1.08

    return round(max(rate, floor), 2), tier


def data_quality_flag(days_ahead: int, bob_quality: str) -> str:
    if days_ahead > 90:
        return 'low'
    if bob_quality == 'high' and days_ahead <= 60:
        return 'high'
    if bob_quality in ('high', 'medium') and days_ahead <= 90:
        return 'medium'
    return 'low'


# ── 7. Assemble output DataFrame ─────────────────────────────────────────────

def assemble_output(
    window:      pd.DataFrame,
    preds_df:    pd.DataFrame,
    bob_quality: str,
) -> pd.DataFrame:
    out = window[['date', 'day_of_week', 'month', 'days_ahead',
                  'floor_price', 'bob_occ', 'pace_gap', 'pickup_velocity',
                  'is_bank_holiday', 'is_local_event']].copy()

    out = pd.concat([out.reset_index(drop=True), preds_df.reset_index(drop=True)],
                    axis=1)

    rates = out.apply(recommend_rate, axis=1)
    out['recommended_rate'] = [r[0] for r in rates]
    out['rate_tier']        = [r[1] for r in rates]

    out['data_quality'] = out.apply(
        lambda r: data_quality_flag(r.get('days_ahead', 90) or 90, bob_quality),
        axis=1,
    )

    out['scored_at']    = datetime.now(timezone.utc).isoformat()
    out['bob_quality']  = bob_quality

    return out


# ── 8. Write to DB (upsert by hotel + date) ───────────────────────────────────

def write_to_db(out: pd.DataFrame, run_id: str, dry_run: bool = False) -> int:
    if dry_run:
        log.info("  DRY RUN — skipping DB write")
        return 0

    conn  = get_db_connection()
    count = 0

    try:
        # Ensure model_run record exists
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO model_runs (
                    run_id, hotel, trained_at,
                    n_training_rows, n_prediction_rows,
                    model_type, promoted
                )
                VALUES (%s, %s, NOW(), 0, %s, 'daily_rescore', TRUE)
                ON CONFLICT (run_id) DO NOTHING
            """, (run_id, HOTEL, len(out)))
        conn.commit()

        with conn.cursor() as cur:
            for _, row in out.iterrows():
                sf = lambda c: float(row[c]) if c in row.index and pd.notna(row[c]) else None
                si = lambda c: int(row[c])   if c in row.index and pd.notna(row[c]) else None

                cur.execute("""
                    INSERT INTO predictions (
                        hotel, run_id, date, day_of_week, month, days_ahead,
                        stage1_occ, pace_adj, bob_adj,
                        predicted_occ, predicted_rooms, occ_low, occ_high,
                        recommended_rate, rate_tier, floor_price,
                        bob_occ, pace_gap, pickup_velocity,
                        is_bank_holiday, is_local_event, data_quality
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,
                        %s,%s,%s, %s,%s,%s, %s,%s,%s
                    )
                    ON CONFLICT (hotel, date, run_id) DO UPDATE SET
                        predicted_occ    = EXCLUDED.predicted_occ,
                        predicted_rooms  = EXCLUDED.predicted_rooms,
                        recommended_rate = EXCLUDED.recommended_rate,
                        rate_tier        = EXCLUDED.rate_tier,
                        pace_adj         = EXCLUDED.pace_adj,
                        bob_adj          = EXCLUDED.bob_adj,
                        data_quality     = EXCLUDED.data_quality
                """, (
                    HOTEL, run_id,
                    row['date'], row.get('day_of_week'),
                    si('month'), si('days_ahead'),
                    sf('stage1_occ'), sf('pace_adj'), sf('bob_adj'),
                    sf('predicted_occ'), si('predicted_rooms'),
                    sf('occ_low'), sf('occ_high'),
                    sf('recommended_rate'), row.get('rate_tier'),
                    sf('floor_price'), sf('bob_occ'),
                    si('pace_gap'), sf('pickup_velocity'),
                    si('is_bank_holiday'), si('is_local_event'),
                    row.get('data_quality'),
                ))
                count += 1

        conn.commit()
        log.info(f"  DB upsert: {count} rows written (run_id={run_id[:8]}...)")

    finally:
        conn.close()

    return count


# ── 9. Save CSV snapshot ──────────────────────────────────────────────────────

def save_csv(out: pd.DataFrame, dry_run: bool = False):
    if dry_run:
        return
    path = PREDICTION_DIR / f"rescore_{date.today().isoformat()}.csv"
    out.to_csv(path, index=False)
    # Also overwrite the live file the API/dashboard reads from
    out.to_csv(MATRIX_FILES['predictions_2026'], index=False)
    log.info(f"  CSV saved → {path.name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(
    days:     int  = RESCORE_DAYS,
    dry_run:  bool = False,
    from_date: date | None = None,
) -> dict:
    import uuid
    run_id     = str(uuid.uuid4())
    start_date = from_date or date.today()
    end_date   = start_date + timedelta(days=days)

    log.info('=' * 60)
    log.info('SmartStay — Daily Re-score')
    log.info(f'Window   : {start_date} → {end_date}  ({days} days)')
    log.info(f'Dry run  : {dry_run}')
    log.info('=' * 60)

    # 1. Find pickup file
    pickup_path, bob_quality, staleness = find_latest_pickup_file()

    if staleness > 7:
        log.warning(
            f"BOB data is {staleness} days old — predictions will use "
            "calendar features only (data_quality='low')"
        )

    # 2. Parse BOB features
    bob_df = None
    if pickup_path:
        log.info(f"\n[Step 1] Parsing BOB features from {pickup_path.name}")
        bob_df = parse_pickup_file(pickup_path)

    # 3. Build feature matrix
    log.info(f"\n[Step 2] Building feature matrix ({start_date} → {end_date})")
    window = build_rescore_matrix(start_date, end_date, bob_df, bob_quality)

    # 4. Load model
    log.info("\n[Step 3] Loading production model")
    gbm, rf = load_model()

    # 5. Predict
    log.info("\n[Step 4] Running Stage 1 + Stage 2 predictions")
    preds_df = predict_and_adjust(window, gbm, rf, bob_quality)

    # 6. Assemble output
    log.info("\n[Step 5] Assembling output + rate recommendations")
    out = assemble_output(window, preds_df, bob_quality)

    # 7. Write DB + CSV
    log.info(f"\n[Step 6] Writing results (dry_run={dry_run})")
    rows_written = write_to_db(out, run_id, dry_run)
    save_csv(out, dry_run)

    # Summary
    log.info('\n' + '=' * 60)
    log.info(f'Re-score COMPLETE')
    log.info(f'  Dates scored   : {len(out)}')
    log.info(f'  BOB quality    : {bob_quality} (staleness: {staleness}d)')
    log.info(f'  Rows in DB     : {rows_written}')
    log.info(f'  Avg predicted  : {out["predicted_occ"].mean():.1%}')
    log.info(f'  Rate tiers     : {out["rate_tier"].value_counts().to_dict()}')
    log.info('=' * 60)

    return {
        'run_id':       run_id,
        'dates_scored': len(out),
        'bob_quality':  bob_quality,
        'rows_written': rows_written,
        'avg_predicted_occ': round(float(out['predicted_occ'].mean()), 4),
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SmartStay Daily Re-score')
    parser.add_argument('--days',     type=int,  default=RESCORE_DAYS,
                        help=f'Days ahead to score (default: {RESCORE_DAYS})')
    parser.add_argument('--dry-run',  action='store_true',
                        help='Preview output — no DB or CSV writes')
    parser.add_argument('--date',     type=str,  default=None,
                        help='Start date YYYY-MM-DD (default: today)')
    args = parser.parse_args()

    from_date = None
    if args.date:
        try:
            from_date = date.fromisoformat(args.date)
        except ValueError:
            log.error(f"Invalid date format: {args.date} — use YYYY-MM-DD")
            sys.exit(1)

    result = main(
        days      = args.days,
        dry_run   = args.dry_run,
        from_date = from_date,
    )
    sys.exit(0)