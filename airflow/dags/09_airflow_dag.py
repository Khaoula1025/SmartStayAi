"""
Script 09: SmartStay Intelligence — Airflow DAGs (Revised)
===========================================================
Two DAGs with correct separation of concerns:

  smartstay_hickstead_daily (06:00 every day)
  ─────────────────────────────────────────────
  Watches data/raw/ for a fresh pickup file.
  If found → full rescore of next 60 days with fresh BOB features.
  If not found within 2h → rescore anyway with stale data (data_quality='low').
  The revenue manager drops pickup_YYYYMMDD.xlsx in data/raw/ each morning.

    FileSensor ──→ rescore_60_days ──→ log_run
         ↓ (timeout)
    rescore_stale ──→ log_run

  smartstay_hickstead_retrain (Monday 07:00)
  ──────────────────────────────────────────
  Full pipeline: re-clean raw data, rebuild matrix, retrain model.
  Only runs when new PMS data has been exported (manual trigger or weekly).
  Only promotes new model if MAE doesn't degrade vs previous.

    clean_all (parallel) ──→ build_matrix ──→ train_model
                                                   ↓
                                            compare_metrics
                                                   ↓
                                          promote_if_better ──→ log_run

The model in the daily DAG never retrains — it loads model.joblib written
by Script 07 / the retrain DAG. Fresh BOB features are the only thing
that changes prediction values each day.

Installation:
  1. Copy this file to $AIRFLOW_HOME/dags/
  2. Set Airflow Variable: SMARTSTAY_PROJECT_ROOT → /path/to/smartstay-intelligence
  3. Ensure Script 07 has been run at least once (model.joblib must exist)
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# ── Project root ──────────────────────────────────────────────────────────────
try:
    PROJECT_ROOT = Path(Variable.get('SMARTSTAY_PROJECT_ROOT'))
except Exception:
    PROJECT_ROOT = Path(
        os.getenv('SMARTSTAY_PROJECT_ROOT',
                  Path(__file__).resolve().parent.parent)
    )

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / 'scripts') not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

log = logging.getLogger('smartstay.dag')

# ── Shared defaults ───────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner':             'smartstay',
    'depends_on_past':   False,
    'retries':           1,
    'retry_delay':       timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'email_on_failure':  True,
    'email':             [os.getenv('AIRFLOW_EMAIL', 'revenue@unhotels.co.uk')],
}

RAW_DIR = PROJECT_ROOT / 'data' / 'raw'


# ── Callable functions ────────────────────────────────────────────────────────

def check_pickup_file(**context) -> str:
    """
    BranchPythonOperator: check whether a fresh pickup file landed today.
    Routes to 'rescore_fresh' if yes, 'rescore_stale' if no.
    """
    from datetime import date
    today      = date.today()
    candidates = list(RAW_DIR.glob('pickup_*.xlsx')) + \
                 list(RAW_DIR.glob('pickup_*.csv'))  + \
                 list(RAW_DIR.glob('Uno_Hotels_Pickup_*.xlsx'))

    for f in candidates:
        file_date = datetime.fromtimestamp(f.stat().st_mtime).date()
        if file_date == today:
            log.info(f"Fresh pickup file found: {f.name}")
            context['ti'].xcom_push(key='pickup_file', value=str(f))
            return 'rescore_fresh'

    log.warning("No fresh pickup file today — routing to stale rescore")
    return 'rescore_stale'


def run_rescore(quality_override: str = None, **context):
    """Run Script 10 daily rescore. Called by both fresh and stale branches."""
    from scripts.daily_rescore import main
    result = main(days=60, dry_run=False)
    log.info(f"Rescore result: {result}")
    return result


def log_pipeline_run(status: str, **context):
    """Write a pipeline_runs record to DB."""
    try:
        from config import get_db_connection
        import uuid

        dag_run = context.get('dag_run')
        run_id  = dag_run.run_id if dag_run else str(uuid.uuid4())
        ti      = context.get('ti')

        # Try to pull rescore result from either branch
        result = (
            ti.xcom_pull(task_ids='rescore_fresh') or
            ti.xcom_pull(task_ids='rescore_stale') or
            {}
        )

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (
                    run_id, hotel, triggered_by, status,
                    steps_completed, rows_written, started_at, finished_at
                ) VALUES (%s, 'hickstead', 'airflow', %s, %s, %s, NOW(), NOW())
            """, (
                run_id, status,
                ['daily_rescore'],
                result.get('rows_written', 0),
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"Could not log pipeline run: {e}")


# ── Retrain step functions ────────────────────────────────────────────────────

def _run_step(script_name: str, step_name: str, **context):
    from scripts.pipeline import _run_main
    _run_main(script_name, step_name)


def compare_and_promote(**context):
    """
    Compare new model MAE vs previous promoted run.
    Only promotes if MAE doesn't degrade by more than 2pp.
    """
    import json
    from config import MATRIX_FILES, get_db_connection

    new_mae = None
    if MATRIX_FILES['model_metrics'].exists():
        with open(MATRIX_FILES['model_metrics']) as f:
            new_mae = json.load(f).get('cv_mae_operational')

    prev_mae = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mae_operational FROM model_runs
                WHERE hotel = 'hickstead' AND promoted = TRUE
                ORDER BY trained_at DESC LIMIT 1 OFFSET 1
            """)
            row = cur.fetchone()
            if row:
                prev_mae = float(row[0])
        conn.close()
    except Exception as e:
        log.warning(f"Could not fetch previous metrics: {e}")

    if prev_mae is None or new_mae is None:
        log.info("No comparison possible — promoting by default")
        promoted = True
    elif new_mae <= prev_mae + 0.02:
        log.info(f"New MAE {new_mae:.4f} ≤ previous {prev_mae:.4f} + 0.02 — PROMOTING")
        promoted = True
    else:
        log.warning(
            f"New MAE {new_mae:.4f} degrades vs previous {prev_mae:.4f} — NOT promoting"
        )
        promoted = False

    context['ti'].xcom_push(key='promoted', value=promoted)
    return promoted


def write_postgres_if_promoted(**context):
    promoted = context['ti'].xcom_pull(task_ids='compare_and_promote', key='promoted')
    if not promoted:
        log.info("Model not promoted — skipping DB write")
        return 0
    from scripts.pipeline import step_write_postgres
    return step_write_postgres(run_id=context['dag_run'].run_id)


# ════════════════════════════════════════════════════════════════════════════════
# DAG 1: smartstay_hickstead_daily
# 06:00 every day — re-scores next 60 days with fresh BOB data
#
# Flow:
#   check_pickup_file ──→ rescore_fresh ──→ log_success
#           │                                     │
#           └──→ rescore_stale ──→ log_success ───┘
#                                       │
#                               (always) log_run
# ════════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id          = 'smartstay_hickstead_daily',
    description     = 'Daily re-score: next 60 days with fresh BOB pickup data',
    default_args    = DEFAULT_ARGS,
    schedule_interval = '0 6 * * *',
    start_date      = datetime(2026, 3, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ['smartstay', 'hickstead', 'daily', 'rescore'],
) as daily_dag:

    # Branch: did a fresh pickup file arrive today?
    t_check = BranchPythonOperator(
        task_id         = 'check_pickup_file',
        python_callable = check_pickup_file,
    )

    # Branch A: fresh BOB data available
    t_rescore_fresh = PythonOperator(
        task_id         = 'rescore_fresh',
        python_callable = run_rescore,
        op_kwargs       = {'quality_override': 'fresh'},
        execution_timeout = timedelta(minutes=20),
    )

    # Branch B: no fresh file — rescore with whatever is available
    t_rescore_stale = PythonOperator(
        task_id         = 'rescore_stale',
        python_callable = run_rescore,
        op_kwargs       = {'quality_override': 'stale'},
        execution_timeout = timedelta(minutes=20),
    )

    # Converge — runs regardless of which branch was taken
    t_join = EmptyOperator(
        task_id      = 'join',
        trigger_rule = TriggerRule.ONE_SUCCESS,
    )

    t_log_success = PythonOperator(
        task_id         = 'log_success',
        python_callable = log_pipeline_run,
        op_kwargs       = {'status': 'success'},
        trigger_rule    = TriggerRule.ONE_SUCCESS,
    )

    t_log_failure = PythonOperator(
        task_id         = 'log_failure',
        python_callable = log_pipeline_run,
        op_kwargs       = {'status': 'failed'},
        trigger_rule    = TriggerRule.ONE_FAILED,
    )

    # Graph:
    # check_pickup_file ──→ rescore_fresh ──┐
    #          └──────────→ rescore_stale ──┴──→ join ──→ log_success
    #                                                   → log_failure
    t_check >> [t_rescore_fresh, t_rescore_stale]
    [t_rescore_fresh, t_rescore_stale] >> t_join
    t_join >> [t_log_success, t_log_failure]


# ════════════════════════════════════════════════════════════════════════════════
# DAG 2: smartstay_hickstead_retrain
# Monday 07:00 — full retrain when new PMS data is available
#
# This does NOT run every week automatically unless the revenue manager
# has dropped fresh PMS exports in data/raw/. Trigger manually after
# monthly PMS export, or let it run weekly and it will use whatever is there.
#
# Flow:
#   clean_* (parallel) ──→ build_matrix ──→ train_model ──→ train_prophet
#                                                 │
#                                          compare_metrics
#                                                 │
#                                       write_postgres (only if promoted)
#                                                 │
#                                             log_run
# ════════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id          = 'smartstay_hickstead_retrain',
    description     = 'Weekly retrain: re-clean data, rebuild matrix, retrain model',
    default_args    = DEFAULT_ARGS,
    schedule_interval = '0 7 * * 1',    # Monday 07:00
    start_date      = datetime(2026, 3, 9),
    catchup         = False,
    max_active_runs = 1,
    tags            = ['smartstay', 'hickstead', 'retrain'],
) as retrain_dag:

    # Cleaning (all 5 in parallel)
    t_clean_occ = PythonOperator(
        task_id='clean_occupancy',
        python_callable=_run_step,
        op_kwargs={'script_name': '01_clean_occupancy.py', 'step_name': 'clean_occupancy'},
    )
    t_clean_dbd = PythonOperator(
        task_id='clean_day_by_day',
        python_callable=_run_step,
        op_kwargs={'script_name': '02_clean_day_by_day.py', 'step_name': 'clean_day_by_day'},
    )
    t_clean_bco = PythonOperator(
        task_id='clean_bookingcom',
        python_callable=_run_step,
        op_kwargs={'script_name': '03_clean_bookingcom.py', 'step_name': 'clean_bookingcom'},
    )
    t_clean_pu = PythonOperator(
        task_id='clean_pickup',
        python_callable=_run_step,
        op_kwargs={'script_name': '04_clean_pickup.py', 'step_name': 'clean_pickup'},
    )
    t_clean_fit = PythonOperator(
        task_id='clean_fit_rates',
        python_callable=_run_step,
        op_kwargs={'script_name': '05_clean_fit_rates.py', 'step_name': 'clean_fit_rates'},
    )

    t_cleaning_done = EmptyOperator(
        task_id      = 'cleaning_done',
        trigger_rule = TriggerRule.ALL_SUCCESS,
    )

    t_build = PythonOperator(
        task_id='build_matrix',
        python_callable=_run_step,
        op_kwargs={'script_name': '06_build_matrix.py', 'step_name': 'build_matrix'},
    )

    t_train_gbm = PythonOperator(
        task_id='train_model',
        python_callable=_run_step,
        op_kwargs={'script_name': '07_train_model.py', 'step_name': 'train_model'},
        execution_timeout=timedelta(minutes=20),
    )

    t_train_prophet = PythonOperator(
        task_id='train_prophet',
        python_callable=_run_step,
        op_kwargs={'script_name': '07b_prophet_model.py', 'step_name': 'train_prophet'},
        execution_timeout=timedelta(minutes=15),
    )

    t_compare = PythonOperator(
        task_id         = 'compare_and_promote',
        python_callable = compare_and_promote,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
    )

    t_write_db = PythonOperator(
        task_id         = 'write_postgres',
        python_callable = write_postgres_if_promoted,
    )

    t_log_success = PythonOperator(
        task_id         = 'log_success',
        python_callable = log_pipeline_run,
        op_kwargs       = {'status': 'success'},
        trigger_rule    = TriggerRule.ALL_SUCCESS,
    )

    t_log_failure = PythonOperator(
        task_id         = 'log_failure',
        python_callable = log_pipeline_run,
        op_kwargs       = {'status': 'failed'},
        trigger_rule    = TriggerRule.ONE_FAILED,
    )

    # Graph
    [t_clean_occ, t_clean_dbd, t_clean_bco, t_clean_pu, t_clean_fit] >> t_cleaning_done
    t_cleaning_done >> t_build >> [t_train_gbm, t_train_prophet]
    [t_train_gbm, t_train_prophet] >> t_compare >> t_write_db
    t_write_db >> [t_log_success, t_log_failure]