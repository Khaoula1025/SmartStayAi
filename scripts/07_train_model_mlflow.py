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
import mlflow
import joblib
import numpy as np
from pathlib import Path
from sklearn.metrics import mean_absolute_error

# ── Import the original training script as-is ─────────────────────────────────
import importlib.util, sys

ROOT        = Path(__file__).resolve().parents[1]
MODELS_DIR  = ROOT / "data" / "models"
DATA_DIR    = ROOT / "data" / "processed"

spec = importlib.util.spec_from_file_location(
    "train_model", ROOT / "scripts" / "07_train_model.py"
)
train_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(train_module)   # runs the script → models saved to disk


# ── Now log everything to MLflow ──────────────────────────────────────────────
mlflow.set_experiment("smartstay_training")

with mlflow.start_run(run_name="ensemble_train"):

    # 1. Tags
    mlflow.set_tags({
        "hotel":      "The Hickstead Hotel",
        "model_type": "GBM_RF_ensemble",
        "script":     "07_train_model.py",
    })

    # 2. Load the saved models and training data to compute metrics
    gbm = joblib.load(MODELS_DIR / "gbm_model.joblib")
    rf  = joblib.load(MODELS_DIR / "rf_model.joblib")

    df = __import__("pandas").read_csv(DATA_DIR / "training_matrix.csv")
    drop = {"date", "hotel", "year", "occupancy_rate"}
    feature_cols = [c for c in df.columns if c not in drop and df[c].dtype in
                    [np.float64, np.int64]]
    X = df[feature_cols].values
    y = df["occupancy_rate"].values

    # 3. Ensemble prediction (60/40)
    y_pred = 0.6 * gbm.predict(X) + 0.4 * rf.predict(X)
    mae    = mean_absolute_error(y, y_pred)
    acc    = float(np.mean(np.abs(y - y_pred) <= 0.10))  # ±10pp threshold

    # 4. Params
    mlflow.log_params({
        "n_training_rows": len(df),
        "n_features":      len(feature_cols),
        "gbm_weight":      0.6,
        "rf_weight":       0.4,
        **{f"gbm_{k}": v for k, v in gbm.get_params().items()},
        **{f"rf_{k}":  v for k, v in rf.get_params().items()},
    })

    # 5. Metrics
    mlflow.log_metrics({
        "mae":      round(mae, 4),
        "accuracy": round(acc, 4),
    })

    # 6. Models
    mlflow.sklearn.log_model(rf,  "rf_model")
    mlflow.lightgbm.log_model(gbm, "gbm_model")

    # 7. Feature importance JSON (if it exists)
    fi_path = MODELS_DIR / "feature_importance.json"
    if fi_path.exists():
        mlflow.log_artifact(str(fi_path))

    print(f"\n✅ MLflow run logged — MAE={mae:.4f}  Accuracy={acc:.1%}")
    print(f"   Run ID: {mlflow.active_run().info.run_id}")
    print("   View:   mlflow ui  →  http://localhost:5000\n")
