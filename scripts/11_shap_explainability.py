"""
Script 11 — SHAP Explainability
================================
Generates SHAP values for every prediction in predictions_2026.csv.
Outputs:
  - data/shap/shap_values.csv         : raw SHAP value per feature per date
  - data/shap/shap_summary.json       : global feature importance (for summary bar chart)
  - data/shap/shap_explanations.json  : per-date top-3 reasons (for dashboard tooltips)

Usage:
  uv run scripts/11_shap_explainability.py
  uv run scripts/11_shap_explainability.py --date 2026-04-15   # single date explanation
  uv run scripts/11_shap_explainability.py --top 5             # show top N features per date
"""

import os, sys, json, argparse
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import shap

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(os.getenv('BASE_DIR',    '.'))
MODELS_DIR   = Path(os.getenv('MODELS_DIR',  'data/models'))
PRED_PATH    = Path(os.getenv('PRED_PATH',   'data/prediction/predictions_2026.csv'))
MATRIX_PATH  = Path(os.getenv('MATRIX_PATH', 'data/processed/prediction_matrix.csv'))
TRAIN_PATH   = Path(os.getenv('TRAIN_PATH',  'data/processed/training_matrix.csv'))
SHAP_DIR     = Path(os.getenv('SHAP_DIR',    'data/shap'))
SHAP_DIR.mkdir(parents=True, exist_ok=True)

# ── Exact feature list from Script 07 ────────────────────────────────────────
FEATURES = [
    'month', 'dow', 'is_weekend', 'is_high_season',
    'cs_occ', 'cs_adr',
    'b_occ', 'b_adr',
    'floor_price',
    'is_bank_holiday', 'is_cultural_holiday', 'is_local_event',
    'season',
]

# Human-readable feature labels for the dashboard
FEATURE_LABELS = {
    'month':               'Month of year',
    'dow':                 'Day of week',
    'is_weekend':          'Weekend',
    'is_high_season':      'High season',
    'cs_occ':              'Competitor occupancy',
    'cs_adr':              'Competitor rate (ADR)',
    'b_occ':               'Budget occupancy target',
    'b_adr':               'Budget rate target',
    'floor_price':         'Floor price (FIT rate)',
    'is_bank_holiday':     'Bank holiday',
    'is_cultural_holiday': 'Cultural holiday',
    'is_local_event':      'Local event nearby',
    'season':              'Season (low/shoulder/high)',
}

# Encoders (must match Script 07 exactly)
SEASON_MAP = {'low': 0, 'shoulder': 1, 'high': 2}
DOW_MAP    = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}


def load_and_encode(path: Path) -> pd.DataFrame:
    """Load matrix CSV and encode categoricals exactly as Script 07 does."""
    df = pd.read_csv(path, parse_dates=['date'])
    df['season'] = df['season'].map(SEASON_MAP).fillna(1).astype(int)
    df['dow']    = df['dow'].map(DOW_MAP).fillna(df['date'].dt.dayofweek).astype(int)
    return df


def direction_label(shap_val: float, feature: str) -> str:
    """Convert a SHAP value to a human-readable direction word."""
    return "↑ increases" if shap_val > 0 else "↓ decreases"


def format_feature_value(feature: str, value: float) -> str:
    """Format a raw feature value into a readable string."""
    if feature in ('is_weekend', 'is_high_season', 'is_bank_holiday',
                   'is_cultural_holiday', 'is_local_event'):
        return 'Yes' if value == 1 else 'No'
    if feature == 'season':
        return {0: 'Low', 1: 'Shoulder', 2: 'High'}.get(int(value), str(value))
    if feature == 'dow':
        return ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][int(value)]
    if feature == 'month':
        return ['Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec'][int(value) - 1]
    if feature in ('cs_occ', 'b_occ'):
        return f"{value:.1%}"
    if feature in ('cs_adr', 'b_adr', 'floor_price'):
        return f"£{value:.0f}"
    return str(round(value, 3))


def main(args):
    print("=" * 60)
    print("Script 11 — SHAP Explainability")
    print("=" * 60)

    # ── Load models ───────────────────────────────────────────────────────────
    print("\n[1/5] Loading models...")
    gbm_path = MODELS_DIR / 'gbm_model.joblib'
    rf_path  = MODELS_DIR / 'rf_model.joblib'

    if not gbm_path.exists():
        sys.exit(f"ERROR: GBM model not found at {gbm_path}\n"
                 "Run 07_patch_joblib.py after Script 07 to save models.")
    if not rf_path.exists():
        sys.exit(f"ERROR: RF model not found at {rf_path}")

    gbm = joblib.load(gbm_path)
    rf  = joblib.load(rf_path)
    print(f"  GBM loaded: {gbm_path}")
    print(f"  RF  loaded: {rf_path}")

    # ── Load prediction matrix ────────────────────────────────────────────────
    print("\n[2/5] Loading prediction matrix...")
    pm = load_and_encode(MATRIX_PATH)

    # Filter to single date if requested
    if args.date:
        pm = pm[pm['date'] == args.date]
        if len(pm) == 0:
            sys.exit(f"ERROR: Date {args.date} not found in prediction matrix.")
        print(f"  Filtered to single date: {args.date}")
    else:
        print(f"  Rows: {len(pm)}")

    X_pred = pm[FEATURES].astype(float)
    dates  = pm['date'].dt.strftime('%Y-%m-%d').values

    # ── Load training matrix (SHAP background) ────────────────────────────────
    print("\n[3/5] Building SHAP background dataset from training matrix...")
    tr = load_and_encode(TRAIN_PATH)
    X_train = tr[FEATURES].astype(float)

    # Use kmeans summary as background (100 samples = fast + accurate enough)
    # This is the recommended approach for TreeExplainer with GBM
    background = shap.maskers.Independent(X_train, max_samples=100)
    print(f"  Background samples: 100 (from {len(X_train)} training rows)")

    # ── Compute SHAP values ───────────────────────────────────────────────────
    print("\n[4/5] Computing SHAP values...")
    print("  Building GBM explainer (TreeExplainer — fast)...")
    gbm_explainer = shap.TreeExplainer(gbm, data=background)
    gbm_shap      = gbm_explainer.shap_values(X_pred)   # shape: (n_dates, n_features)

    print("  Building RF explainer...")
    rf_explainer  = shap.TreeExplainer(rf, data=background)
    rf_shap       = rf_explainer.shap_values(X_pred)

    # Ensemble SHAP values with same 60/40 weights as the model
    ensemble_shap = 0.6 * gbm_shap + 0.4 * rf_shap
    base_value    = 0.6 * gbm_explainer.expected_value + 0.4 * rf_explainer.expected_value

    print(f"  SHAP matrix shape: {ensemble_shap.shape}")
    print(f"  Base value (mean training occupancy): {base_value:.3f} ({base_value:.1%})")

    # ── Build outputs ─────────────────────────────────────────────────────────
    print("\n[5/5] Building output files...")

    # --- A: shap_values.csv (raw matrix, one row per date) -------------------
    shap_df = pd.DataFrame(ensemble_shap, columns=FEATURES)
    shap_df.insert(0, 'date', dates)
    shap_df['base_value'] = base_value

    # Add prediction from predictions_2026.csv for cross-reference
    if not args.date:
        pred_csv = pd.read_csv(PRED_PATH, parse_dates=['date'])
        pred_csv['date_str'] = pred_csv['date'].dt.strftime('%Y-%m-%d')
        shap_df = shap_df.merge(
            pred_csv[['date_str', 'predicted_occ', 'recommended_rate', 'rate_tier']].rename(
                columns={'date_str': 'date'}),
            on='date', how='left'
        )

    shap_csv_path = SHAP_DIR / 'shap_values.csv'
    shap_df.to_csv(shap_csv_path, index=False)
    print(f"  Saved: {shap_csv_path} ({len(shap_df)} rows)")

    # --- B: shap_summary.json (global feature importance) --------------------
    # Mean absolute SHAP value per feature across all dates
    mean_abs_shap = np.abs(ensemble_shap).mean(axis=0)
    feature_importance = [
        {
            "feature":      f,
            "label":        FEATURE_LABELS[f],
            "importance":   round(float(mean_abs_shap[i]), 5),
            "importance_pp": round(float(mean_abs_shap[i]) * 100, 2),  # in percentage points
        }
        for i, f in enumerate(FEATURES)
    ]
    feature_importance.sort(key=lambda x: x['importance'], reverse=True)

    summary = {
        "base_value":          round(float(base_value), 4),
        "base_value_pct":      round(float(base_value) * 100, 1),
        "n_dates":             int(len(dates)),
        "n_features":          len(FEATURES),
        "feature_importance":  feature_importance,
        "generated_at":        pd.Timestamp.now().isoformat(),
    }

    summary_path = SHAP_DIR / 'shap_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved: {summary_path}")

    # --- C: shap_explanations.json (per-date top-N reasons) ------------------
    top_n = args.top
    explanations = {}

    for i, date in enumerate(dates):
        row_shap   = ensemble_shap[i]
        row_values = X_pred.iloc[i]

        # Sort features by absolute SHAP value (most impactful first)
        ranked = sorted(
            enumerate(row_shap),
            key=lambda x: abs(x[1]),
            reverse=True
        )

        reasons = []
        for feat_idx, shap_val in ranked[:top_n]:
            feature    = FEATURES[feat_idx]
            feat_value = row_values.iloc[feat_idx]
            reasons.append({
                "feature":       feature,
                "label":         FEATURE_LABELS[feature],
                "shap_value":    round(float(shap_val), 5),
                "shap_value_pp": round(float(shap_val) * 100, 2),
                "direction":     "up" if shap_val > 0 else "down",
                "feature_value": format_feature_value(feature, feat_value),
                "explanation":   (
                    f"{FEATURE_LABELS[feature]} ({format_feature_value(feature, feat_value)}) "
                    f"{direction_label(shap_val, feature)} predicted occupancy "
                    f"by {abs(shap_val)*100:.1f}pp"
                )
            })

        # Prediction info
        pred_occ  = None
        pred_rate = None
        rate_tier = None
        if 'predicted_occ' in shap_df.columns:
            row_pred = shap_df[shap_df['date'] == date]
            if len(row_pred):
                pred_occ  = row_pred['predicted_occ'].iloc[0]
                pred_rate = row_pred['recommended_rate'].iloc[0]
                rate_tier = row_pred['rate_tier'].iloc[0]
                pred_occ  = None if pd.isna(pred_occ)  else round(float(pred_occ), 4)
                pred_rate = None if pd.isna(pred_rate) else round(float(pred_rate), 2)

        # Waterfall: cumulative SHAP from base to final
        sorted_shap = sorted(row_shap, key=abs, reverse=True)
        cumulative  = float(base_value)
        waterfall   = [{"label": "Base (avg occupancy)", "value": round(cumulative, 4), "cumulative": round(cumulative, 4)}]
        for feat_idx, shap_val in ranked[:top_n]:
            cumulative += float(shap_val)
            waterfall.append({
                "label":      FEATURE_LABELS[FEATURES[feat_idx]],
                "value":      round(float(shap_val), 4),
                "cumulative": round(cumulative, 4)
            })

        explanations[date] = {
            "date":          date,
            "predicted_occ": pred_occ,
            "predicted_occ_pct": round(pred_occ * 100, 1) if pred_occ is not None else None,
            "recommended_rate": pred_rate,
            "rate_tier":     rate_tier,
            "base_value":    round(float(base_value), 4),
            "top_reasons":   reasons,
            "waterfall":     waterfall,
            "shap_sum":      round(float(row_shap.sum()), 5),
            "model_output":  round(float(base_value + row_shap.sum()), 4),
        }

    explanations_path = SHAP_DIR / 'shap_explanations.json'
    with open(explanations_path, 'w') as f:
        json.dump(explanations, f, indent=2)
    print(f"  Saved: {explanations_path} ({len(explanations)} date entries)")

    # ── Print summary to console ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("GLOBAL FEATURE IMPORTANCE (mean |SHAP| in pp)")
    print("=" * 60)
    for item in feature_importance:
        bar = "█" * int(item['importance_pp'] * 2)
        print(f"  {item['label']:<30} {item['importance_pp']:>5.1f}pp  {bar}")

    # Print single-date explanation if requested
    if args.date and args.date in explanations:
        exp = explanations[args.date]
        print(f"\n{'='*60}")
        print(f"EXPLANATION FOR {args.date}")
        print(f"{'='*60}")
        print(f"  Predicted occupancy: {exp['predicted_occ_pct']}%")
        print(f"  Recommended rate:    £{exp['recommended_rate']}")
        print(f"  Rate tier:           {exp['rate_tier']}")
        print(f"  Base value:          {exp['base_value']*100:.1f}%")
        print(f"\n  Top {top_n} reasons:")
        for r in exp['top_reasons']:
            sign = "+" if r['direction'] == 'up' else ""
            print(f"    {sign}{r['shap_value_pp']:+.1f}pp  {r['label']} = {r['feature_value']}")
        print(f"\n  Waterfall check: base({exp['base_value']*100:.1f}%) + SHAP sum = {exp['model_output']*100:.1f}%")

    print(f"\n✅ SHAP complete. Files in: {SHAP_DIR}/")
    print(f"   shap_values.csv        — raw SHAP matrix")
    print(f"   shap_summary.json      — global feature importance")
    print(f"   shap_explanations.json — per-date top-{top_n} reasons")
    print(f"\nNext step: add GET /api/v1/explain/{{date}} endpoint to FastAPI")
    print(f"           to serve shap_explanations.json to the dashboard.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SmartStay SHAP Explainability')
    parser.add_argument('--date', type=str, default=None,
                        help='Single date to explain (YYYY-MM-DD). Omit for all dates.')
    parser.add_argument('--top', type=int, default=3,
                        help='Number of top reasons per date (default: 3)')
    args = parser.parse_args()
    main(args)
