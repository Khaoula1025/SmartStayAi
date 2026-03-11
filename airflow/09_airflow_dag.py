import sys
import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# ── Resolve project root ──────────────────────────────────────────────────────
# Try Airflow Variable first, fall back to env var, then relative to this file
try:
    PROJECT_ROOT = Path(Variable.get('SMARTSTAY_PROJECT_ROOT'))
except Exception:
    PROJECT_ROOT = Path(
        os.getenv('SMARTSTAY_PROJECT_ROOT', Path(__file__).resolve().parent.parent)
    )

# Add project root to sys.path so config.py and scripts/ are importable
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / 'scripts') not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

# ── Import pipeline functions ─────────────────────────────────────────────────
# Each step function is imported directly — no subprocess calls
from scripts.pipeline import (          # noqa: E402
    step_clean_occupancy,
    step_clean_day_by_day,
    step_clean_bookingcom,
    step_clean_pickup,
    step_clean_fit_rates,
    step_build_matrix,
    step_train_model,
    step_train_prophet,
    step_validate,
    step_write_postgres,
    step_log_run,
)
from config import (                    # noqa: E402
    AIRFLOW_DAG_ID, AIRFLOW_RETRAIN_ID,
    AIRFLOW_OWNER, AIRFLOW_EMAIL_ALERTS,
    MATRIX_FILES, get_logger,
)

log = get_logger('smartstay.airflow')

# ── Shared DAG defaults ───────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner':              AIRFLOW_OWNER,
    'depends_on_past':    False,
    'email':              [AIRFLOW_EMAIL_ALERTS],
    'email_on_failure':   True,
    'email_on_retry':     False,
    'retries':            1,
    'retry_delay':        timedelta(minutes=5),
    'execution_timeout':  timedelta(minutes=30),
}


# ── Helper: wrap step functions for Airflow context ──────────────────────────
def _make_task(fn):
    """
    Wraps a pipeline step function to accept Airflow's **context kwargs.
    The wrapped function logs start/end and pushes return value to XCom.
    """
    def wrapper(**context):
        log.info(f"Starting task: {fn.__name__}")
        result = fn(**context)
        log.info(f"Completed task: {fn.__name__}  result={result}")
        return result
    wrapper.__name__ = fn.__name__
    wrapper.__doc__  = fn.__doc__
    return wrapper


# ── Retrain comparison function ───────────────────────────────────────────────
def compare_and_promote(**context):
    """
    Weekly retrain only — compare new model metrics against the previous
    promoted run. Only promotes the new model if MAE does not degrade
    by more than 2pp.

    Pushes 'promoted' (bool) to XCom for downstream tasks.
    """
    ti = context['ti']

    # Pull metrics from the train_model task output (XCom)
    new_metrics_raw = ti.xcom_pull(task_ids='train_model')
    new_mae = new_metrics_raw.get('mae_operational') if new_metrics_raw else None

    # Load previous best metrics from DB
    prev_mae = None
    try:
        from config import get_db_connection
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mae_operational FROM model_runs
                WHERE hotel = 'hickstead' AND promoted = TRUE
                ORDER BY trained_at DESC
                LIMIT 1 OFFSET 1
            """)
            row = cur.fetchone()
            if row:
                prev_mae = float(row[0])
        conn.close()
    except Exception as e:
        log.warning(f"Could not fetch previous metrics: {e}")

    # Decision
    if prev_mae is None:
        log.info("No previous promoted model found — promoting new model by default")
        promoted = True
    elif new_mae is None:
        log.warning("New model MAE not available — skipping promotion")
        promoted = False
    elif new_mae <= prev_mae + 0.02:   # allow up to 2pp degradation
        log.info(f"New MAE {new_mae:.4f} vs previous {prev_mae:.4f} — PROMOTING")
        promoted = True
    else:
        log.warning(
            f"New MAE {new_mae:.4f} degrades by "
            f"{(new_mae-prev_mae)*100:.1f}pp vs previous {prev_mae:.4f} — NOT promoting"
        )
        promoted = False

    ti.xcom_push(key='promoted', value=promoted)
    return promoted


def write_postgres_if_promoted(**context):
    """Only write to DB if the new model was promoted."""
    ti       = context['ti']
    promoted = ti.xcom_pull(task_ids='compare_and_promote', key='promoted')

    if not promoted:
        log.info("Model not promoted — skipping DB write")
        return 0

    run_id = context['dag_run'].run_id
    return step_write_postgres(run_id=run_id, **context)


def log_pipeline_success(**context):
    dag_run = context['dag_run']
    step_log_run(
        run_id=dag_run.run_id,
        status='success',
        steps_completed=context.get('task_instance_key_str', '').split(','),
        steps_failed=[],
        triggered_by='airflow',
    )


def log_pipeline_failure(**context):
    dag_run = context['dag_run']
    step_log_run(
        run_id=dag_run.run_id,
        status='failed',
        steps_completed=[],
        steps_failed=[context.get('task_instance_key_str', 'unknown')],
        triggered_by='airflow',
        error_message=str(context.get('exception', '')),
    )


# ════════════════════════════════════════════════════════════════════
# DAG 1: smartstay_hickstead_daily
# Runs every day at 06:00 UTC
# Full pipeline: ingest → clean → build → train → validate → write DB
# ════════════════════════════════════════════════════════════════════
with DAG(
    dag_id=AIRFLOW_DAG_ID,
    description='SmartStay — daily pipeline for Hickstead Hotel',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 6 * * *',   # 06:00 UTC every day
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=['smartstay', 'hickstead', 'daily'],
) as daily_dag:

    # ── Cleaning tasks (run in parallel — no dependencies between them) ──
    t_clean_occupancy = PythonOperator(
        task_id='clean_occupancy',
        python_callable=_make_task(step_clean_occupancy),
    )
    t_clean_day_by_day = PythonOperator(
        task_id='clean_day_by_day',
        python_callable=_make_task(step_clean_day_by_day),
    )
    t_clean_bookingcom = PythonOperator(
        task_id='clean_bookingcom',
        python_callable=_make_task(step_clean_bookingcom),
    )
    t_clean_pickup = PythonOperator(
        task_id='clean_pickup',
        python_callable=_make_task(step_clean_pickup),
    )
    t_clean_fit_rates = PythonOperator(
        task_id='clean_fit_rates',
        python_callable=_make_task(step_clean_fit_rates),
    )

    # ── Cleaning gate: all 5 cleaning tasks must pass before matrix build
    t_cleaning_done = EmptyOperator(
        task_id='cleaning_done',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Matrix build ─────────────────────────────────────────────────────
    t_build_matrix = PythonOperator(
        task_id='build_matrix',
        python_callable=_make_task(step_build_matrix),
    )

    # ── Model training (GBM and Prophet run in parallel) ─────────────────
    t_train_model = PythonOperator(
        task_id='train_model',
        python_callable=_make_task(step_train_model),
        execution_timeout=timedelta(minutes=15),
    )
    t_train_prophet = PythonOperator(
        task_id='train_prophet',
        python_callable=_make_task(step_train_prophet),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Validate ──────────────────────────────────────────────────────────
    t_validate = PythonOperator(
        task_id='validate',
        python_callable=_make_task(step_validate),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Write to PostgreSQL ───────────────────────────────────────────────
    t_write_postgres = PythonOperator(
        task_id='write_postgres',
        python_callable=lambda **ctx: step_write_postgres(
            run_id=ctx['dag_run'].run_id, **ctx
        ),
    )

    # ── Pipeline log (always runs — even on failure) ──────────────────────
    t_log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_pipeline_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    t_log_failure = PythonOperator(
        task_id='log_failure',
        python_callable=log_pipeline_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Task graph ────────────────────────────────────────────────────────
    #
    #  clean_occupancy  ──┐
    #  clean_day_by_day   │
    #  clean_bookingcom   ├── cleaning_done ── build_matrix ──┬── train_model  ──┐
    #  clean_pickup       │                                   └── train_prophet  ──┤
    #  clean_fit_rates  ──┘                                                        │
    #                                                                          validate
    #                                                                              │
    #                                                                       write_postgres
    #                                                                           │     │
    #                                                                      log_success log_failure

    [
        t_clean_occupancy,
        t_clean_day_by_day,
        t_clean_bookingcom,
        t_clean_pickup,
        t_clean_fit_rates,
    ] >> t_cleaning_done >> t_build_matrix >> [t_train_model, t_train_prophet]

    [t_train_model, t_train_prophet] >> t_validate >> t_write_postgres

    t_write_postgres >> [t_log_success, t_log_failure]


# ════════════════════════════════════════════════════════════════════
# DAG 2: smartstay_hickstead_retrain_weekly
# Runs every Monday at 07:00 UTC
# Rebuilds matrices + retrains both models + promotes only if accuracy holds
# ════════════════════════════════════════════════════════════════════
with DAG(
    dag_id=AIRFLOW_RETRAIN_ID,
    description='SmartStay — weekly retrain for Hickstead Hotel',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 7 * * 1',   # 07:00 UTC every Monday
    start_date=datetime(2026, 3, 9),
    catchup=False,
    max_active_runs=1,
    tags=['smartstay', 'hickstead', 'retrain'],
) as retrain_dag:

    # ── Matrix rebuild ────────────────────────────────────────────────────
    rt_build_matrix = PythonOperator(
        task_id='build_matrix',
        python_callable=_make_task(step_build_matrix),
    )

    # ── Retrain both models ───────────────────────────────────────────────
    rt_train_model = PythonOperator(
        task_id='train_model',
        python_callable=_make_task(step_train_model),
        execution_timeout=timedelta(minutes=15),
    )
    rt_train_prophet = PythonOperator(
        task_id='train_prophet',
        python_callable=_make_task(step_train_prophet),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Validate ──────────────────────────────────────────────────────────
    rt_validate = PythonOperator(
        task_id='validate',
        python_callable=_make_task(step_validate),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Compare metrics and decide whether to promote ─────────────────────
    rt_compare = PythonOperator(
        task_id='compare_and_promote',
        python_callable=compare_and_promote,
    )

    # ── Write to DB only if model was promoted ────────────────────────────
    rt_write_postgres = PythonOperator(
        task_id='write_postgres',
        python_callable=write_postgres_if_promoted,
    )

    # ── Log ───────────────────────────────────────────────────────────────
    rt_log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_pipeline_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    rt_log_failure = PythonOperator(
        task_id='log_failure',
        python_callable=log_pipeline_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Task graph ────────────────────────────────────────────────────────
    #
    #  build_matrix ──┬── train_model  ──┐
    #                 └── train_prophet  ──── validate ── compare_and_promote
    #                                                             │
    #                                                      write_postgres
    #                                                          │      │
    #                                                     log_success log_failure

    rt_build_matrix >> [rt_train_model, rt_train_prophet]
    [rt_train_model, rt_train_prophet] >> rt_validate >> rt_compare >> rt_write_postgres
    rt_write_postgres >> [rt_log_success, rt_log_failure]
