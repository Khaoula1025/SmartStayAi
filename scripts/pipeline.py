import os
import sys
import json
import uuid
import argparse
import importlib.util
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

# ── Project root on path so config.py is importable ──────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import (
    SCRIPTS_DIR, CLEAN_FILES, MATRIX_FILES,
    HOTEL, get_db_connection, get_logger,
)

log = get_logger('smartstay.pipeline')


# ── Script importer ───────────────────────────────────────────────────────────
def _run_main(filename: str, step_name: str):
    """
    Dynamically import a script from SCRIPTS_DIR and call its main() function.
    Replaces subprocess calls — runs in the same Python process so exceptions
    propagate correctly to Airflow and the CLI.
    """
    path = SCRIPTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Script not found: {path}")

    log.info(f"  Importing {filename} ...")
    spec   = importlib.util.spec_from_file_location(
                 filename.replace('.py', '').replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module

    try:
        spec.loader.exec_module(module)
        if hasattr(module, 'main'):
            module.main()
        log.info(f"  {filename} — done")
    except Exception as e:
        raise RuntimeError(f"Step '{step_name}' failed in {filename}: {e}") from e


# ── Step functions (each callable independently by Airflow) ──────────────────

def step_clean_occupancy(**context):
    """Step 1 — Clean raw occupancy Excel files."""
    _run_main('01_clean_occupancy.py', 'clean_occupancy')
    return str(CLEAN_FILES['occupancy'])


def step_clean_day_by_day(**context):
    """Step 2 — Clean day-by-day budget split."""
    _run_main('02_clean_day_by_day.py', 'clean_day_by_day')
    return str(CLEAN_FILES['day_by_day'])


def step_clean_bookingcom(**context):
    """Step 3 — Clean Booking.com competitor rates."""
    _run_main('03_clean_bookingcom.py', 'clean_bookingcom')
    return str(CLEAN_FILES['bookingcom'])


def step_clean_pickup(**context):
    """Step 4 — Clean BOB pickup snapshot."""
    _run_main('04_clean_pickup.py', 'clean_pickup')
    return str(CLEAN_FILES['pickup'])


def step_clean_fit_rates(**context):
    """Step 5 — Clean FIT static rates and build floor price calendar."""
    _run_main('05_clean_fit_rates.py', 'clean_fit_rates')
    return str(CLEAN_FILES['fit_rates'])


def step_build_matrix(**context):
    """Step 6 — Build training and prediction matrices."""
    _run_main('06_build_matrix.py', 'build_matrix')
    tr = pd.read_csv(MATRIX_FILES['training'])
    pr = pd.read_csv(MATRIX_FILES['prediction'])
    log.info(f"  Training matrix  : {len(tr)} rows x {tr.shape[1]} cols")
    log.info(f"  Prediction matrix: {len(pr)} rows x {pr.shape[1]} cols")
    return {'training_rows': len(tr), 'prediction_rows': len(pr)}


def step_train_model(**context):
    """Step 7 — Train GBM+RF ensemble, generate predictions, save model."""
    _run_main('07_train_model.py', 'train_model')
    preds   = pd.read_csv(MATRIX_FILES['predictions_2026'])
    metrics = {}
    if MATRIX_FILES['model_metrics'].exists():
        with open(MATRIX_FILES['model_metrics']) as f:
            metrics = json.load(f)
    log.info(f"  Predictions: {len(preds)} rows")
    log.info(f"  Operational MAE: {metrics.get('cv_mae_operational')}  "
             f"Accuracy: {metrics.get('occ_accuracy_pct')}%")
    return {
        'n_predictions':    len(preds),
        'mae_operational':  metrics.get('cv_mae_operational'),
        'occ_accuracy_pct': metrics.get('occ_accuracy_pct'),
    }


def step_train_prophet(**context):
    """Step 7b — Train Prophet model and produce comparison report."""
    _run_main('07b_prophet_model.py', 'train_prophet')
    return str(MATRIX_FILES['model_comparison'])


# ── Step 8: Validate outputs ──────────────────────────────────────────────────
VALIDATION_RULES = {
    'clean_occupancy': {
        'path':          CLEAN_FILES['occupancy'],
        'min_rows':      300,
        'required_cols': ['date', 'occ_rate', 'rooms_let', 'tot_rooms'],
        'no_null_cols':  ['date', 'occ_rate'],
    },
    'clean_pickup': {
        'path':          CLEAN_FILES['pickup'],
        'min_rows':      100,
        'required_cols': ['date', 'bob_sold', 'bob_occ', 'pace_gap', 'days_ahead'],
        'no_null_cols':  ['date', 'bob_occ'],
    },
    'training_matrix': {
        'path':          MATRIX_FILES['training'],
        'min_rows':      600,
        'required_cols': ['date', 'occ_rate', 'month', 'dow', 'floor_price'],
        'no_null_cols':  ['date', 'occ_rate'],
    },
    'prediction_matrix': {
        'path':          MATRIX_FILES['prediction'],
        'min_rows':      100,
        'required_cols': ['date', 'bob_occ', 'pace_gap', 'b_occ', 'floor_price'],
        'no_null_cols':  ['date'],
    },
    'predictions_2026': {
        'path':          MATRIX_FILES['predictions_2026'],
        'min_rows':      100,
        'required_cols': ['date', 'predicted_occ', 'predicted_rooms',
                          'recommended_rate', 'rate_tier', 'occ_low', 'occ_high'],
        'no_null_cols':  ['date', 'predicted_occ', 'recommended_rate'],
        'range_checks':  {
            'predicted_occ':    (0.0, 1.0),
            'recommended_rate': (40.0, 300.0),
        },
    },
}


def step_validate(**context) -> dict:
    """
    Step 8 — Validate all pipeline output CSVs.
    Raises ValueError if any file fails (Airflow marks task as failed).
    """
    log.info("Validating pipeline outputs ...")
    results    = {}
    all_passed = True

    for name, rules in VALIDATION_RULES.items():
        path   = rules['path']
        result = {'file': name, 'passed': True, 'issues': []}

        if not path.exists():
            result['passed'] = False
            result['issues'].append('File not found')
            all_passed = False
            log.warning(f"  MISSING : {name}")
            results[name] = result
            continue

        df = pd.read_csv(path)

        if len(df) < rules['min_rows']:
            result['issues'].append(
                f"Only {len(df)} rows (expected >= {rules['min_rows']})"
            )
            result['passed'] = False

        for col in rules.get('required_cols', []):
            if col not in df.columns:
                result['issues'].append(f"Missing column: {col}")
                result['passed'] = False

        for col in rules.get('no_null_cols', []):
            if col in df.columns and df[col].isna().sum() > 0:
                result['issues'].append(
                    f"{col} has {df[col].isna().sum()} nulls"
                )
                result['passed'] = False

        for col, (lo, hi) in rules.get('range_checks', {}).items():
            if col in df.columns:
                out = ((df[col] < lo) | (df[col] > hi)).sum()
                if out > 0:
                    result['issues'].append(
                        f"{col}: {out} values outside [{lo},{hi}]"
                    )
                    result['passed'] = False

        result['row_count'] = len(df)
        results[name]       = result
        status = 'OK  ' if result['passed'] else 'FAIL'
        issues = ' | '.join(result['issues']) if result['issues'] else 'all checks passed'
        log.info(f"  [{status}] {name:<25} {len(df):>5} rows — {issues}")

        if not result['passed']:
            all_passed = False

    if not all_passed:
        failed = [k for k, v in results.items() if not v['passed']]
        raise ValueError(f"Validation failed: {failed}")

    log.info("  All outputs valid")
    return results


# ── Step 9: Write to PostgreSQL ───────────────────────────────────────────────
def step_write_postgres(run_id: str = None, **context) -> int:
    """
    Step 9 — Write predictions and actuals to PostgreSQL.
    Returns number of prediction rows written.
    """
    if run_id is None:
        dr = context.get('dag_run')
        run_id = dr.run_id if dr and hasattr(dr, 'run_id') else str(uuid.uuid4())

    log.info(f"Writing to PostgreSQL (run_id={run_id[:8]}...) ...")
    conn         = get_db_connection()
    rows_written = 0

    try:
        # ── model_runs ─────────────────────────────────────────────────────
        metrics = {}
        if MATRIX_FILES['model_metrics'].exists():
            with open(MATRIX_FILES['model_metrics']) as f:
                metrics = json.load(f)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO model_runs (
                    run_id, hotel, trained_at,
                    n_training_rows, n_prediction_rows,
                    mae_operational, mae_all_folds, r2_mean, occ_accuracy_pct,
                    features, model_type, stage2_regime, promoted
                ) VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                run_id, HOTEL,
                metrics.get('n_training_rows', 0),
                metrics.get('n_prediction_rows', 0),
                metrics.get('cv_mae_operational'),
                metrics.get('cv_mae_all_folds'),
                metrics.get('cv_r2_mean'),
                metrics.get('occ_accuracy_pct'),
                metrics.get('features', []),
                metrics.get('model', ''),
                metrics.get('stage2_regime', 'moderate'),
                True,
            ))
        conn.commit()
        log.info("  model_runs — inserted")

        # ── predictions ────────────────────────────────────────────────────
        preds = pd.read_csv(MATRIX_FILES['predictions_2026'], parse_dates=['date'])

        with conn.cursor() as cur:
            for _, row in preds.iterrows():
                sf = lambda c: float(row[c]) if c in row and pd.notna(row[c]) else None
                si = lambda c: int(row[c])   if c in row and pd.notna(row[c]) else None
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
                        recommended_rate = EXCLUDED.recommended_rate
                """, (
                    HOTEL, run_id,
                    row['date'].date(), row.get('day_of_week'),
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
                rows_written += 1
        conn.commit()
        log.info(f"  predictions — {rows_written} rows")

        # ── actuals ────────────────────────────────────────────────────────
        if CLEAN_FILES['occupancy'].exists():
            occ = pd.read_csv(CLEAN_FILES['occupancy'], parse_dates=['date'])
            with conn.cursor() as cur:
                for _, row in occ.iterrows():
                    sf = lambda c: float(row[c]) if c in row and pd.notna(row[c]) else None
                    si = lambda c: int(row[c])   if c in row and pd.notna(row[c]) else None
                    cur.execute("""
                        INSERT INTO actuals (
                            hotel, date, occ_rate, rooms_let, tot_rooms,
                            avl, occ_pct,
                            db_let, db_occ_rate, db_sb_let, db_sb_occ_rate,
                            exec_let, exec_occ_rate, tb_let, tb_occ_rate,
                            is_interpolated
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (hotel, date) DO UPDATE SET
                            occ_rate=EXCLUDED.occ_rate,
                            rooms_let=EXCLUDED.rooms_let,
                            loaded_at=NOW()
                    """, (
                        HOTEL, row['date'].date(),
                        sf('occ_rate'), si('rooms_let'), si('tot_rooms'),
                        si('avl'), sf('occ_pct'),
                        si('DB_let'), sf('DB_occ_rate'),
                        si('DB_SB_let'), sf('DB_SB_occ_rate'),
                        si('EXEC_let'), sf('EXEC_occ_rate'),
                        si('TB_let'), sf('TB_occ_rate'),
                        bool(row.get('is_interpolated', False)),
                    ))
            conn.commit()
            log.info(f"  actuals — {len(occ)} rows")

    finally:
        conn.close()

    return rows_written


# ── Step 10: Log pipeline run ─────────────────────────────────────────────────
def step_log_run(
    run_id: str,
    status: str,
    steps_completed: list,
    steps_failed: list,
    rows_written: int = 0,
    triggered_by: str = 'manual',
    error_message: str = None,
    **context,
):
    """Step 10 — Write pipeline_runs record to PostgreSQL."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (
                    run_id, hotel, triggered_by, status,
                    steps_completed, steps_failed,
                    rows_written, error_message,
                    started_at, finished_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
            """, (
                run_id, HOTEL, triggered_by, status,
                steps_completed, steps_failed,
                rows_written, error_message,
            ))
        conn.commit()
        conn.close()
        log.info(f"  pipeline_runs — status={status}")
    except Exception as e:
        log.warning(f"  Could not write pipeline_runs record: {e}")


# ── Full pipeline orchestrator ────────────────────────────────────────────────
STEP_REGISTRY = [
    (1,  'clean_occupancy',  step_clean_occupancy),
    (2,  'clean_day_by_day', step_clean_day_by_day),
    (3,  'clean_bookingcom', step_clean_bookingcom),
    (4,  'clean_pickup',     step_clean_pickup),
    (5,  'clean_fit_rates',  step_clean_fit_rates),
    (6,  'build_matrix',     step_build_matrix),
    (7,  'train_model',      step_train_model),
    (8,  'train_prophet',    step_train_prophet),
    (9,  'validate',         step_validate),
    (10, 'write_postgres',   None),   # handled inline
]


def run_pipeline(
    steps: list = None,
    dry_run: bool = False,
    skip_db: bool = False,
) -> bool:
    run_id        = str(uuid.uuid4())
    steps_to_run  = steps or list(range(1, 11))
    completed     = []
    failed        = []
    rows_written  = 0
    error_message = None

    log.info('=' * 60)
    log.info('SmartStay Intelligence — ETL Pipeline')
    log.info(f'Run ID   : {run_id}')
    log.info(f'Hotel    : {HOTEL}')
    log.info(f'Steps    : {steps_to_run}')
    log.info(f'Dry run  : {dry_run}  |  Skip DB: {skip_db or dry_run}')
    log.info('=' * 60)

    start = datetime.now(timezone.utc)

    try:
        for num, name, fn in STEP_REGISTRY:
            if num not in steps_to_run or num == 10:
                continue
            log.info(f'\n[Step {num}] {name}')
            fn()
            completed.append(name)

        # Step 10: DB write
        if 10 in steps_to_run and not dry_run and not skip_db:
            log.info('\n[Step 10] write_postgres')
            rows_written = step_write_postgres(run_id=run_id)
            completed.append('write_postgres')
        else:
            log.info('\n[Step 10] write_postgres — SKIPPED')

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        log.info('\n' + '=' * 60)
        log.info(f'COMPLETED in {elapsed:.1f}s  |  rows written: {rows_written}')
        log.info('=' * 60)

        if not dry_run and not skip_db:
            step_log_run(run_id, 'success', completed, failed, rows_written)

        return True

    except Exception as e:
        error_message = str(e)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        log.error(f'\nFAILED after {elapsed:.1f}s: {error_message}')
        log.error(f'Completed: {completed}')

        if not dry_run and not skip_db:
            step_log_run(run_id, 'failed', completed, failed,
                         error_message=error_message)
        return False


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SmartStay ETL Pipeline')
    parser.add_argument('--steps', type=str, default=None,
                        help='Comma-separated step numbers e.g. "6,7,9,10"')
    parser.add_argument('--dry-run', action='store_true',
                        help='Validate only — no DB writes')
    parser.add_argument('--skip-db', action='store_true',
                        help='Run steps 1-9, skip DB write')
    args = parser.parse_args()

    steps = None
    if args.steps:
        try:
            steps = [int(s.strip()) for s in args.steps.split(',')]
        except ValueError:
            log.error('--steps must be comma-separated integers')
            sys.exit(1)

    success = run_pipeline(
        steps=steps, dry_run=args.dry_run, skip_db=args.skip_db
    )
    sys.exit(0 if success else 1)
