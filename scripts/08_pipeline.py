import os
import sys
import json
import uuid
import argparse
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('smartstay.pipeline')

# Paths — override via env vars or .env file
SCRIPTS_DIR = Path(os.getenv('SCRIPTS_DIR', '/app/scripts'))
DATA_DIR    = Path(os.getenv('DATA_DIR',    '/app/data/raw'))
OUTPUT_DIR  = Path(os.getenv('OUTPUT_DIR',  '/app/data/processed'))
METRICS_PATH = OUTPUT_DIR / 'model_metrics.json'

# DB connection — reads from env vars
DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = os.getenv('DB_PORT', '5432')
DB_NAME     = os.getenv('DB_NAME', 'smartstay')
DB_USER     = os.getenv('DB_USER', 'smartstay')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

HOTEL       = 'hickstead'

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db_connection():
    """Return a psycopg2 connection. Imported lazily so pipeline
    can run in --skip-db mode without psycopg2 installed."""
    try:
        import psycopg2
        return psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            connect_timeout=10,
        )
    except ImportError:
        raise RuntimeError(
            "psycopg2 not installed. Run: pip install psycopg2-binary"
        )
    except Exception as e:
        raise RuntimeError(f"Cannot connect to PostgreSQL: {e}")


def db_execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


# ── Step runners ──────────────────────────────────────────────────────────────
def run_script(script_name: str, step_num: int) -> bool:
    """Run a numbered pipeline script as a subprocess.
    Returns True on success, False on failure."""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False

    log.info(f"Step {step_num}: Running {script_name} ...")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'DATA_DIR':   str(DATA_DIR),
            'OUTPUT_DIR': str(OUTPUT_DIR),
        },
    )

    if result.returncode != 0:
        log.error(f"  FAILED — {script_name}")
        log.error(f"  stderr: {result.stderr[-1000:]}")
        return False

    # Print key output lines (non-empty, not separator lines)
    for line in result.stdout.splitlines():
        if line.strip() and not line.startswith('='):
            log.info(f"  {line}")
    return True


# ── Step 1–7: cleaning + model ────────────────────────────────────────────────
def step_clean_occupancy():
    return run_script('01_clean_occupancy.py', 1)

def step_clean_day_by_day():
    return run_script('02_clean_day_by_day.py', 2)

def step_clean_bookingcom():
    return run_script('03_clean_bookingcom.py', 3)

def step_clean_pickup():
    return run_script('04_clean_pickup.py', 4)

def step_clean_fit_rates():
    return run_script('05_clean_fit_rates.py', 5)

def step_build_matrix():
    return run_script('06_build_matrix.py', 6)

def step_train_model():
    return run_script('07_train_model.py', 7)


# ── Step 8: validate outputs ──────────────────────────────────────────────────
VALIDATION_RULES = {
    'clean_occupancy.csv': {
        'min_rows': 300,
        'required_cols': ['date', 'occ_rate', 'rooms_let', 'tot_rooms'],
        'no_null_cols':  ['date', 'occ_rate'],
    },
    'clean_pickup.csv': {
        'min_rows': 100,
        'required_cols': ['date', 'bob_sold', 'bob_occ', 'pace_gap', 'days_ahead'],
        'no_null_cols':  ['date', 'bob_occ'],
    },
    'training_matrix.csv': {
        'min_rows': 600,
        'required_cols': ['date', 'occ_rate', 'month', 'dow', 'floor_price'],
        'no_null_cols':  ['date', 'occ_rate'],
    },
    'prediction_matrix.csv': {
        'min_rows': 100,
        'required_cols': ['date', 'bob_occ', 'pace_gap', 'b_occ', 'floor_price'],
        'no_null_cols':  ['date'],
    },
    'predictions_2026.csv': {
        'min_rows': 100,
        'required_cols': ['date', 'predicted_occ', 'predicted_rooms',
                          'recommended_rate', 'rate_tier', 'occ_low', 'occ_high'],
        'no_null_cols':  ['date', 'predicted_occ', 'recommended_rate'],
        'range_checks': {
            'predicted_occ':  (0.0, 1.0),
            'recommended_rate': (40.0, 300.0),
        },
    },
}


def step_validate_outputs() -> dict:
    """
    Validates all pipeline output CSVs.
    Returns a dict with passed=True/False and details per file.
    """
    log.info("Step 8: Validating pipeline outputs ...")
    results = {}
    all_passed = True

    for filename, rules in VALIDATION_RULES.items():
        path = OUTPUT_DIR / filename
        file_result = {'file': filename, 'passed': True, 'issues': []}

        if not path.exists():
            file_result['passed'] = False
            file_result['issues'].append('File not found')
            results[filename] = file_result
            all_passed = False
            log.warning(f"  ✗ {filename} — FILE NOT FOUND")
            continue

        try:
            df = pd.read_csv(path)
        except Exception as e:
            file_result['passed'] = False
            file_result['issues'].append(f'Cannot read CSV: {e}')
            results[filename] = file_result
            all_passed = False
            continue

        # Row count
        if len(df) < rules['min_rows']:
            issue = f"Only {len(df)} rows (expected ≥ {rules['min_rows']})"
            file_result['issues'].append(issue)
            file_result['passed'] = False

        # Required columns
        for col in rules.get('required_cols', []):
            if col not in df.columns:
                file_result['issues'].append(f"Missing column: {col}")
                file_result['passed'] = False

        # No-null columns
        for col in rules.get('no_null_cols', []):
            if col in df.columns:
                nulls = df[col].isna().sum()
                if nulls > 0:
                    file_result['issues'].append(f"{col} has {nulls} nulls")
                    file_result['passed'] = False

        # Range checks
        for col, (lo, hi) in rules.get('range_checks', {}).items():
            if col in df.columns:
                out_of_range = ((df[col] < lo) | (df[col] > hi)).sum()
                if out_of_range > 0:
                    file_result['issues'].append(
                        f"{col}: {out_of_range} values outside [{lo}, {hi}]"
                    )
                    file_result['passed'] = False

        file_result['row_count'] = len(df)
        results[filename] = file_result
        status = '✓' if file_result['passed'] else '✗'
        issues_str = ' | '.join(file_result['issues']) if file_result['issues'] else 'OK'
        log.info(f"  {status} {filename:<35} {len(df):>5} rows  {issues_str}")

        if not file_result['passed']:
            all_passed = False

    log.info(f"  Validation {'PASSED ✓' if all_passed else 'FAILED ✗'}")
    return {'passed': all_passed, 'details': results}


# ── Step 9: write to PostgreSQL ───────────────────────────────────────────────
def step_write_to_postgres(run_id: str) -> int:
    """
    Writes predictions and actuals to PostgreSQL.
    Returns the number of prediction rows written.
    """
    log.info("Step 9: Writing to PostgreSQL ...")

    conn = get_db_connection()
    rows_written = 0

    try:
        # ── 9a. Insert model_run record ────────────────────────────────────
        metrics = {}
        if METRICS_PATH.exists():
            with open(METRICS_PATH) as f:
                metrics = json.load(f)

        features = metrics.get('features', [])
        db_execute(conn, """
            INSERT INTO model_runs (
                run_id, hotel, trained_at, n_training_rows, n_prediction_rows,
                mae_operational, mae_all_folds, r2_mean, occ_accuracy_pct,
                features, model_type, stage2_regime, promoted, notes
            ) VALUES (
                %s, %s, NOW(), %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
        """, (
            run_id,
            HOTEL,
            metrics.get('n_training_rows', 0),
            metrics.get('n_prediction_rows', 0),
            metrics.get('cv_mae_operational'),
            metrics.get('cv_mae_all_folds'),
            metrics.get('cv_r2_mean'),
            metrics.get('occ_accuracy_pct'),
            features,
            metrics.get('model', ''),
            metrics.get('stage2_regime', 'moderate'),
            True,   # auto-promote for now; add comparison logic later
            None,
        ))
        log.info(f"  model_runs record inserted (run_id={run_id[:8]}...)")

        # ── 9b. Insert predictions ─────────────────────────────────────────
        pred_path = OUTPUT_DIR / 'predictions_2026.csv'
        preds = pd.read_csv(pred_path, parse_dates=['date'])

        insert_pred_sql = """
            INSERT INTO predictions (
                hotel, run_id, date, day_of_week, month, days_ahead,
                stage1_occ, pace_adj, bob_adj,
                predicted_occ, predicted_rooms, occ_low, occ_high,
                recommended_rate, rate_tier, floor_price,
                bob_occ, pace_gap, pickup_velocity,
                is_bank_holiday, is_local_event, data_quality
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (hotel, date, run_id) DO UPDATE SET
                predicted_occ    = EXCLUDED.predicted_occ,
                predicted_rooms  = EXCLUDED.predicted_rooms,
                recommended_rate = EXCLUDED.recommended_rate,
                rate_tier        = EXCLUDED.rate_tier,
                occ_low          = EXCLUDED.occ_low,
                occ_high         = EXCLUDED.occ_high
        """

        with conn.cursor() as cur:
            for _, row in preds.iterrows():
                cur.execute(insert_pred_sql, (
                    HOTEL,
                    run_id,
                    row['date'].date() if hasattr(row['date'], 'date') else row['date'],
                    row.get('day_of_week'),
                    int(row['month']) if pd.notna(row.get('month')) else None,
                    int(row['days_ahead']) if pd.notna(row.get('days_ahead')) else None,
                    float(row['stage1_occ'])       if pd.notna(row.get('stage1_occ'))      else None,
                    float(row['pace_adj'])          if pd.notna(row.get('pace_adj'))         else None,
                    float(row['bob_adj'])           if pd.notna(row.get('bob_adj'))          else None,
                    float(row['predicted_occ']),
                    int(row['predicted_rooms']),
                    float(row['occ_low'])           if pd.notna(row.get('occ_low'))         else None,
                    float(row['occ_high'])          if pd.notna(row.get('occ_high'))        else None,
                    float(row['recommended_rate']),
                    row.get('rate_tier'),
                    float(row['floor_price'])       if pd.notna(row.get('floor_price'))     else None,
                    float(row['bob_occ'])           if pd.notna(row.get('bob_occ'))         else None,
                    int(row['pace_gap'])            if pd.notna(row.get('pace_gap'))        else None,
                    float(row['pickup_velocity'])   if pd.notna(row.get('pickup_velocity')) else None,
                    int(row.get('is_bank_holiday', 0)),
                    int(row.get('is_local_event',  0)),
                    row.get('data_quality'),
                ))
                rows_written += 1
        conn.commit()
        log.info(f"  predictions: {rows_written} rows written")

        # ── 9c. Insert actuals (occupancy history) ─────────────────────────
        occ_path = OUTPUT_DIR / 'clean_occupancy.csv'
        if occ_path.exists():
            occ = pd.read_csv(occ_path, parse_dates=['date'])
            actuals_written = 0

            insert_act_sql = """
                INSERT INTO actuals (
                    hotel, date, occ_rate, rooms_let, tot_rooms, avl, occ_pct,
                    db_let, db_occ_rate, db_sb_let, db_sb_occ_rate,
                    exec_let, exec_occ_rate, tb_let, tb_occ_rate,
                    is_interpolated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s
                )
                ON CONFLICT (hotel, date) DO UPDATE SET
                    occ_rate    = EXCLUDED.occ_rate,
                    rooms_let   = EXCLUDED.rooms_let,
                    loaded_at   = NOW()
            """

            with conn.cursor() as cur:
                for _, row in occ.iterrows():
                    def safe_float(col):
                        v = row.get(col)
                        return float(v) if pd.notna(v) else None
                    def safe_int(col):
                        v = row.get(col)
                        return int(v) if pd.notna(v) else None

                    cur.execute(insert_act_sql, (
                        HOTEL,
                        row['date'].date(),
                        safe_float('occ_rate'),
                        safe_int('rooms_let'),
                        safe_int('tot_rooms'),
                        safe_int('avl'),
                        safe_float('occ_pct'),
                        safe_int('DB_let'),
                        safe_float('DB_occ_rate'),
                        safe_int('DB_SB_let'),
                        safe_float('DB_SB_occ_rate'),
                        safe_int('EXEC_let'),
                        safe_float('EXEC_occ_rate'),
                        safe_int('TB_let'),
                        safe_float('TB_occ_rate'),
                        bool(row.get('is_interpolated', False)),
                    ))
                    actuals_written += 1
            conn.commit()
            log.info(f"  actuals: {actuals_written} rows written")

    finally:
        conn.close()

    return rows_written


# ── Step 10: log pipeline run ─────────────────────────────────────────────────
def step_log_pipeline_run(
    run_id: str,
    status: str,
    steps_completed: list,
    steps_failed: list,
    rows_written: int,
    error_message: str = None,
):
    """Write a pipeline_runs record to PostgreSQL."""
    log.info(f"Step 10: Logging pipeline run (status={status}) ...")
    try:
        conn = get_db_connection()
        db_execute(conn, """
            INSERT INTO pipeline_runs (
                run_id, hotel, triggered_by, status,
                steps_completed, steps_failed,
                rows_written, error_message,
                started_at, finished_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                NOW(), NOW()
            )
        """, (
            run_id, HOTEL, 'manual', status,
            steps_completed, steps_failed,
            rows_written, error_message,
        ))
        conn.close()
        log.info("  pipeline_runs record written")
    except Exception as e:
        log.warning(f"  Could not write pipeline_runs record: {e}")


# ── Main orchestrator ─────────────────────────────────────────────────────────
ALL_STEPS = {
    1: ('clean_occupancy',  step_clean_occupancy),
    2: ('clean_day_by_day', step_clean_day_by_day),
    3: ('clean_bookingcom', step_clean_bookingcom),
    4: ('clean_pickup',     step_clean_pickup),
    5: ('clean_fit_rates',  step_clean_fit_rates),
    6: ('build_matrix',     step_build_matrix),
    7: ('train_model',      step_train_model),
    8: ('validate',         None),   # handled inline
    9: ('write_postgres',   None),   # handled inline
}


def run_pipeline(
    steps: list = None,
    dry_run: bool = False,
    skip_db: bool = False,
) -> bool:
    """
    Run the SmartStay ETL pipeline.

    Args:
        steps:    List of step numbers to run (default: all 1–9)
        dry_run:  If True, run validation only — no DB writes
        skip_db:  If True, run steps 1–8 but skip step 9

    Returns:
        True if all steps passed, False if any step failed.
    """
    run_id = str(uuid.uuid4())
    steps_to_run = steps or list(range(1, 10))
    steps_completed = []
    steps_failed    = []
    rows_written    = 0
    error_message   = None

    log.info("=" * 60)
    log.info("SmartStay Intelligence — ETL Pipeline")
    log.info(f"Run ID   : {run_id}")
    log.info(f"Hotel    : {HOTEL}")
    log.info(f"Steps    : {steps_to_run}")
    log.info(f"Dry run  : {dry_run}")
    log.info(f"Skip DB  : {skip_db or dry_run}")
    log.info("=" * 60)

    start = datetime.now(timezone.utc)

    try:
        # Steps 1–7: run external scripts
        for step_num in [s for s in steps_to_run if s <= 7]:
            step_name, step_fn = ALL_STEPS[step_num]
            ok = step_fn()
            if ok:
                steps_completed.append(step_name)
            else:
                steps_failed.append(step_name)
                error_message = f"Step {step_num} ({step_name}) failed"
                log.error(f"Pipeline aborted at step {step_num}: {step_name}")
                break

        if steps_failed:
            raise RuntimeError(error_message)

        # Step 8: validate
        if 8 in steps_to_run:
            validation = step_validate_outputs()
            if not validation['passed']:
                failed_files = [
                    k for k, v in validation['details'].items()
                    if not v['passed']
                ]
                error_message = f"Validation failed: {failed_files}"
                steps_failed.append('validate')
                raise RuntimeError(error_message)
            steps_completed.append('validate')

        # Step 9: write to DB
        if 9 in steps_to_run and not dry_run and not skip_db:
            rows_written = step_write_to_postgres(run_id)
            steps_completed.append('write_postgres')
        elif dry_run or skip_db:
            log.info("Step 9: Skipped (dry_run or skip_db)")

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        log.info("=" * 60)
        log.info(f"Pipeline COMPLETED in {elapsed:.1f}s")
        log.info(f"Steps completed : {steps_completed}")
        log.info(f"Rows written    : {rows_written}")
        log.info("=" * 60)

        # Step 10: log success
        if not dry_run and not skip_db:
            step_log_pipeline_run(
                run_id, 'success', steps_completed,
                steps_failed, rows_written,
            )

        return True

    except Exception as e:
        error_message = str(e)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        log.error("=" * 60)
        log.error(f"Pipeline FAILED after {elapsed:.1f}s: {error_message}")
        log.error(f"Steps completed : {steps_completed}")
        log.error(f"Steps failed    : {steps_failed}")
        log.error("=" * 60)

        if not dry_run and not skip_db:
            step_log_pipeline_run(
                run_id, 'failed', steps_completed,
                steps_failed, rows_written, error_message,
            )

        return False


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='SmartStay ETL Pipeline'
    )
    parser.add_argument(
        '--steps',
        type=str,
        default=None,
        help='Comma-separated step numbers to run, e.g. "6,7,8,9"'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run pipeline but skip all DB writes'
    )
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='Run pipeline steps 1–8 but skip step 9 (DB write)'
    )
    args = parser.parse_args()

    steps = None
    if args.steps:
        try:
            steps = [int(s.strip()) for s in args.steps.split(',')]
        except ValueError:
            log.error("--steps must be comma-separated integers, e.g. '6,7,8,9'")
            sys.exit(1)

    success = run_pipeline(
        steps=steps,
        dry_run=args.dry_run,
        skip_db=args.skip_db,
    )
    sys.exit(0 if success else 1)
