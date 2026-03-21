"""
07_train_model_mlflow.py — SmartStay Intelligence
MLflow wrapper around 07_train_model.py.

Runs the existing training script and logs everything to MLflow.
07_train_model.py is NOT modified.

Usage:
    cd ~/Desktop/smartstay-intelligence
    uv run python scripts/07_train_model_mlflow.py

Then inspect results:
    mlflow ui
    → http://localhost:5000
"""

import json
import importlib.util
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
import joblib
from pathlib import Path

ROOT       = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "data" / "models"
PRED_DIR   = ROOT / "data" / "prediction"

# ── Step 1: Run 07_train_model.py unchanged ────────────────────────────────────
spec = importlib.util.spec_from_file_location(
    "train_model", ROOT / "scripts" / "07_train_model.py"
)
train_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(train_module)   # models saved to disk as normal


# ── Step 2: Read metrics that the script already computed and saved ────────────
metrics_path = PRED_DIR / "model_metrics.json"
if not metrics_path.exists():
    raise FileNotFoundError(f"model_metrics.json not found at {metrics_path}")

with open(metrics_path) as f:
    m = json.load(f)

print("\n[MLflow] Metrics loaded from model_metrics.json:")
print(json.dumps(m, indent=2))


# ── Step 3: Log to MLflow ──────────────────────────────────────────────────────
mlflow.set_experiment("smartstay_training")

with mlflow.start_run(run_name="ensemble_train"):

    # Tags
    mlflow.set_tags({
        "hotel":      "The Hickstead Hotel",
        "model_type": "GBM_RF_ensemble",
        "script":     "07_train_model.py",
    })

    # Params — read from the saved models so nothing is hardcoded here
    gbm = joblib.load(MODELS_DIR / "gbm_model.joblib")
    rf  = joblib.load(MODELS_DIR / "rf_model.joblib")

    mlflow.log_params({
        "gbm_weight": 0.6,
        "rf_weight":  0.4,
        **{f"gbm_{k}": v for k, v in gbm.get_params().items()},
        **{f"rf_{k}":  v for k, v in rf.get_params().items()},
    })

    # Flat numeric metrics (cv_mae_operational, occ_accuracy_pct, etc.)
    mlflow.log_metrics({k: float(v) for k, v in m.items() if isinstance(v, (int, float))})

    # Feature importances — each feature logged as fi_<name>
    if "feature_importances" in m:
        mlflow.log_metrics({f"fi_{k}": v for k, v in m["feature_importances"].items()})

    # Feature list — logged as a single comma-separated param
    if "features" in m:
        mlflow.log_param("features", ",".join(m["features"]))

    # Models (native MLflow flavours — enables model registry + serving)
    mlflow.lightgbm.log_model(gbm, "gbm_model")
    mlflow.sklearn.log_model(rf,   "rf_model")

    run_id = mlflow.active_run().info.run_id
    print(f"\n✅ MLflow run logged — Run ID: {run_id}")
    print("   View: mlflow ui  →  http://localhost:5000\n")