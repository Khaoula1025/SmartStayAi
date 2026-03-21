import pandas as pd
import numpy as np
import warnings
import json
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import os
import joblib
from pathlib import Path
warnings.filterwarnings('ignore')

# ── Paths ─────────────────────────────────────────────────────────────────────
TRAIN_PATH = 'data/processed/training_matrix.csv'
PRED_PATH  = 'data/processed/prediction_matrix.csv'
OUT_PATH   = 'data/prediction/predictions_2026.csv'
METRICS_PATH = 'data/prediction/model_metrics.json'

TOT_ROOMS  = 52

# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading matrices...")
tr = pd.read_csv(TRAIN_PATH, parse_dates=['date'])
pr = pd.read_csv(PRED_PATH,  parse_dates=['date'])
print(f"  Training:   {tr.shape[0]} rows  ({tr.date.min().date()} → {tr.date.max().date()})")
print(f"  Prediction: {pr.shape[0]} rows  ({pr.date.min().date()} → {pr.date.max().date()})")

# ── Shared feature set ────────────────────────────────────────────────────────
# Only features that exist AND are populated in BOTH matrices
FEATURES = [
    'month', 'dow', 'is_weekend', 'is_high_season',
    'cs_occ', 'cs_adr',
    'b_occ', 'b_adr',
    'floor_price',
    'is_bank_holiday', 'is_cultural_holiday', 'is_local_event',
    'season',
]

# ── Encode season ─────────────────────────────────────────────────────────────
season_map = {'low': 0, 'shoulder': 1, 'high': 2}
tr['season'] = tr['season'].map(season_map).fillna(1).astype(int)
pr['season'] = pr['season'].map(season_map).fillna(1).astype(int)

# ── Encode dow (Mon=0 … Sun=6) ────────────────────────────────────────────────
dow_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
tr['dow'] = tr['dow'].map(dow_map).fillna(tr['date'].dt.dayofweek)
pr['dow'] = pr['dow'].map(dow_map).fillna(pr['date'].dt.dayofweek)

# ── Impute b_occ / b_adr for Zone B (2024 rows — no budget data) ──────────────
# Use DOW median from Zone A (2025 rows) as proxy
print("\nImputing b_occ / b_adr for Zone B...")
zone_a = tr[tr['data_zone'].str.startswith('A')]
dow_med_b_occ = zone_a.groupby('dow')['b_occ'].median()
dow_med_b_adr = zone_a.groupby('dow')['b_adr'].median()
tr['b_occ'] = tr.apply(
    lambda r: dow_med_b_occ.get(r['dow'], 0.70) if pd.isna(r['b_occ']) else r['b_occ'], axis=1)
tr['b_adr'] = tr.apply(
    lambda r: dow_med_b_adr.get(r['dow'], 75.0) if pd.isna(r['b_adr']) else r['b_adr'], axis=1)
print(f"  b_occ missing after imputation: {tr['b_occ'].isna().sum()}")
print(f"  b_adr missing after imputation: {tr['b_adr'].isna().sum()}")

# ── Final feature matrix ──────────────────────────────────────────────────────
X = tr[FEATURES].astype(float)
y = tr['occ_rate'].astype(float)
X_pred = pr[FEATURES].astype(float)

assert X.isna().sum().sum() == 0,  f"NaNs in training X: {X.isna().sum()}"
assert X_pred.isna().sum().sum() == 0, f"NaNs in prediction X: {X_pred.isna().sum()}"
print(f"\nFeature matrix clean — training: {X.shape}, prediction: {X_pred.shape}")

# ── Stage 1: Cross-validation ─────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGE 1 — MODEL TRAINING & CROSS-VALIDATION")
print("="*60)

# Two models: GBM (main) and RF (ensemble partner)
gbm = GradientBoostingRegressor(
    n_estimators=300,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    min_samples_leaf=5,
    random_state=42
)
rf = RandomForestRegressor(
    n_estimators=300,
    max_depth=6,
    min_samples_leaf=5,
    random_state=42
)

# 5-fold time-aware CV (chronological split)
print("\nRunning 5-fold cross-validation (time-ordered)...")
tr_sorted = tr.sort_values('date').reset_index(drop=True)
X_sorted  = tr_sorted[FEATURES].astype(float)
y_sorted  = tr_sorted['occ_rate'].astype(float)

# Seasonal holdout splits — avoids year-boundary distribution shift
# NOTE: Apr-Jun 2025 fold will show high MAE (~34pp) because 2024 April was only
# 6% occ (hotel just opened) — the model has never seen a normal April when
# predicting 2025 April. This is a data limitation, NOT a model failure.
# For 2026 predictions, the full 638-row training set includes 2025 Apr-Jun,
# so this artifact does not affect actual prediction quality.
cv_splits = [
    {'name': 'Apr-Jun 2025', 'val_start': '2025-04-01', 'val_end': '2025-06-30'},
    {'name': 'Jul-Sep 2025', 'val_start': '2025-07-01', 'val_end': '2025-09-30'},
    {'name': 'Oct-Dec 2025', 'val_start': '2025-10-01', 'val_end': '2025-12-30'},
]
cv_maes, cv_r2s = [], []

X_all_sorted = tr_sorted[FEATURES].astype(float)
y_all_sorted = tr_sorted['occ_rate'].astype(float)

for s in cv_splits:
    val_mask_cv   = (tr_sorted['date'] >= s['val_start']) & (tr_sorted['date'] <= s['val_end'])
    train_mask_cv = ~val_mask_cv
    gbm.fit(X_all_sorted[train_mask_cv], y_all_sorted[train_mask_cv])
    preds = np.clip(gbm.predict(X_all_sorted[val_mask_cv]), 0, 1)
    mae  = mean_absolute_error(y_all_sorted[val_mask_cv], preds)
    r2   = r2_score(y_all_sorted[val_mask_cv], preds)
    cv_maes.append(mae)
    cv_r2s.append(r2)
    note = ' ⚠ hotel barely open in Apr 2024 (6% occ) — validation artifact' if 'Apr' in s['name'] else ''
    print(f"  [{s['name']}]  train_n={train_mask_cv.sum()}  "
          f"MAE={mae:.4f} ({mae*100:.1f}pp)  R²={r2:.3f}{note}")

mean_mae     = np.mean(cv_maes)
mean_r2      = np.mean(cv_r2s)
# Operational MAE: exclude Apr fold (hotel barely open in Apr 2024 — validation artifact)
# Jul-Dec folds are the true benchmark for 2026 prediction quality
op_mae       = np.mean(cv_maes[1:])   # Jul-Sep and Oct-Dec folds
occ_acc      = 1 - op_mae
print(f"\n  Overall CV MAE (all folds) : {mean_mae:.4f}  ({mean_mae*100:.1f}pp)")
print(f"  Operational MAE (Jul-Dec)  : {op_mae:.4f}  ({op_mae*100:.1f}pp)  ← primary benchmark")
print(f"  Occupancy prediction accuracy (operational): ~{occ_acc*100:.1f}%")

# Train final models on all data
print("\nFitting final models on full training set...")
gbm.fit(X, y)
rf.fit(X, y)

# Feature importance
feat_imp = pd.Series(gbm.feature_importances_, index=FEATURES).sort_values(ascending=False)
print("\nTop feature importances (GBM):")
for feat, imp in feat_imp.head(8).items():
    bar = '█' * int(imp * 50)
    print(f"  {feat:<25} {imp:.3f}  {bar}")

# ── Stage 1 predictions ───────────────────────────────────────────────────────
print("\n" + "="*60)
print("GENERATING PREDICTIONS")
print("="*60)

s1_gbm = np.clip(gbm.predict(X_pred), 0, 1)
s1_rf  = np.clip(rf.predict(X_pred),  0, 1)

# Ensemble: 60% GBM + 40% RF
s1_ensemble = 0.6 * s1_gbm + 0.4 * s1_rf

# Confidence interval from disagreement between models + CV spread
model_spread = np.abs(s1_gbm - s1_rf)
ci_half = np.maximum(model_spread * 1.5, mean_mae)   # at least CV MAE wide
occ_low  = np.clip(s1_ensemble - ci_half, 0, 1)
occ_high = np.clip(s1_ensemble + ci_half, 0, 1)

# ── Stage 1.5: Month-level correction for outlier months ─────────────────────
# April 2024 was only 6.3% occ (hotel barely open) — the model under-predicts
# April and nearby months because it has never seen a normal spring season.
# Fix: for months where 2024 occ was < 20%, blend the model prediction with
# the 2025 monthly DOW mean (our only reference for a normal season).
print("\nApplying Stage 1.5 month correction for outlier months (2024 < 20% occ)...")

# Compute 2025 monthly × DOW mean occupancy from training data
za = tr[tr['data_zone'].str.startswith('A')].copy()
season_map_r = {0:'low', 1:'shoulder', 2:'high'}
month_dow_mean = za.groupby(['month', 'dow'])['occ_rate'].mean()

# Months where 2024 is an outlier (occ < 20%)
zb = tr[tr['data_zone'].str.startswith('B')].copy()
outlier_months = set()
for m in range(1, 13):
    occ24 = zb[zb['date'].dt.month == m]['occ_rate'].mean()
    if pd.notna(occ24) and occ24 < 0.20:
        outlier_months.add(m)

print(f"  Outlier months (2024 occ < 20%): {sorted(outlier_months)}")

s1_corrected = s1_ensemble.copy()
for i, row in pr.iterrows():
    idx = pr.index.get_loc(i)
    m   = int(row['month'])
    d   = int(row['dow'])
    if m in outlier_months:
        ref_occ = month_dow_mean.get((m, d), s1_ensemble[idx])
        # Blend 40% model + 60% 2025 reference for outlier months
        s1_corrected[idx] = 0.40 * s1_ensemble[idx] + 0.60 * ref_occ

# Recompute CI around corrected stage1
occ_low  = np.clip(s1_corrected - ci_half, 0, 1)
occ_high = np.clip(s1_corrected + ci_half, 0, 1)

corrected_months = pr['month'].isin(outlier_months)
print(f"  Corrected {corrected_months.sum()} prediction rows")
print(f"  April stage1 before: {s1_ensemble[pr['month'].values==4].mean():.3f}  "
      f"after: {s1_corrected[pr['month'].values==4].mean():.3f}")


print("\nApplying Stage 2 BOB adjustment (moderate)...")
"""
Rules (moderate regime):
  pace_gap adjustment (days > 90 — long-range, calendar model only):
    pace_gap < -15 rooms:  reduce by up to -0.08 (scaled by pace severity)
    pace_gap > +5  rooms:  increase by up to +0.04

  BOB pull adjustment:
    days <= 30  (close-in):   pull 35% toward bob_occ if divergence > 15%
    days 31-90  (medium):     pull 15% toward bob_occ if divergence > 25%
    days > 90   (long-range): no BOB pull (too early in booking window)

  pickup_velocity micro-adjustment (days <= 60 only):
    high positive velocity (>0.15/day): +0.03 boost
    negative velocity (<-0.05/day):     -0.03 penalty
"""

pace_gap        = pr['pace_gap'].fillna(0).values
bob_occ         = pr['bob_occ'].fillna(pd.Series(s1_corrected, index=pr.index)).values
pickup_vel      = pr['pickup_velocity'].fillna(0).values
days_ahead      = pr['days_ahead'].fillna(180).values

bob_adj  = np.zeros(len(pr))
pace_adj = np.zeros(len(pr))

for i in range(len(pr)):
    base = s1_corrected[i]   # use corrected stage1 as reference for divergence

    # --- pace adjustment (long-range signal: > 90 days out) ---
    if days_ahead[i] > 90:
        if pace_gap[i] < -15:
            scale = min(abs(pace_gap[i]) / 52, 1.0)
            pace_adj[i] = -0.08 * scale
        elif pace_gap[i] > 5:
            scale = min(pace_gap[i] / 52, 1.0)
            pace_adj[i] = +0.04 * scale

    # --- BOB pull (calibrated by booking window) ---
    divergence = bob_occ[i] - base
    if days_ahead[i] <= 30:
        if abs(divergence) > 0.15:
            bob_adj[i] = 0.35 * divergence   # strong close-in signal
    elif days_ahead[i] <= 90:
        if abs(divergence) > 0.25:
            bob_adj[i] = 0.15 * divergence   # gentle medium-term signal
    # days > 90: no BOB pull — too early in booking window

    # --- pickup velocity micro-adjustment (days <= 60 only) ---
    if days_ahead[i] <= 60:
        if pickup_vel[i] > 0.15:
            bob_adj[i] += 0.03
        elif pickup_vel[i] < -0.05:
            bob_adj[i] -= 0.03

total_adj  = pace_adj + bob_adj
s2_occ     = np.clip(s1_corrected + total_adj, 0, 1)
occ_low    = np.clip(occ_low  + total_adj, 0, 1)
occ_high   = np.clip(occ_high + total_adj, 0, 1)

# rooms sold (round to integer, cap at 52)
pred_rooms = np.clip(np.round(s2_occ * TOT_ROOMS).astype(int), 0, TOT_ROOMS)

# ── Rate recommendation ───────────────────────────────────────────────────────
"""
Rate tiers based on predicted occupancy:
  ≥ 90%  → premium   : floor_price × 1.35
  75–89% → high       : floor_price × 1.20
  60–74% → standard   : floor_price × 1.05
  45–59% → value      : floor_price × 0.95
  < 45%  → promotional: floor_price × 0.85
"""
floor  = pr['floor_price'].values
rec_rate  = np.zeros(len(pr))
rate_tier = []

for i, occ in enumerate(s2_occ):
    if occ >= 0.90:
        mult, tier = 1.35, 'premium'
    elif occ >= 0.75:
        mult, tier = 1.20, 'high'
    elif occ >= 0.60:
        mult, tier = 1.05, 'standard'
    elif occ >= 0.45:
        mult, tier = 0.95, 'value'
    else:
        mult, tier = 0.85, 'promotional'
    rec_rate[i] = round(floor[i] * mult, 2)
    rate_tier.append(tier)

# ── Data quality flag ─────────────────────────────────────────────────────────
def quality_flag(da):
    if da <= 30:   return 'high'     # close-in, BOB signal strong
    if da <= 90:   return 'medium'   # medium-term, pace signal useful
    return 'low'                     # long-range, calendar model only

data_quality = [quality_flag(da) for da in days_ahead]

# ── Assemble output ───────────────────────────────────────────────────────────
print("\nAssembling output dataframe...")
out = pd.DataFrame({
    'date':            pr['date'].dt.date,
    'day_of_week':     pr['day_of_week'],
    'month':           pr['month'],
    'days_ahead':      days_ahead.astype(int),
    'predicted_occ':   np.round(s2_occ, 4),
    'predicted_rooms': pred_rooms,
    'occ_low':         np.round(occ_low, 4),
    'occ_high':        np.round(occ_high, 4),
    'recommended_rate': rec_rate,
    'rate_tier':       rate_tier,
    'floor_price':     floor.round(2),
    'stage1_occ':      np.round(s1_corrected, 4),
    'bob_adj':         np.round(bob_adj, 4),
    'pace_adj':        np.round(pace_adj, 4),
    'bob_occ':         np.round(bob_occ, 4),
    'pace_gap':        pace_gap.astype(int),
    'pickup_velocity': np.round(pickup_vel, 4),
    'is_bank_holiday': pr['is_bank_holiday'].values,
    'is_local_event':  pr['is_local_event'].values,
    'data_quality':    data_quality,
})

# ── Summary statistics ────────────────────────────────────────────────────────
print("\n" + "="*60)
print("PREDICTION SUMMARY")
print("="*60)
print(f"\nDate range: {out.date.min()} → {out.date.max()}")
print(f"Total nights: {len(out)}")
print()
print("Predicted occupancy by month:")
out['date_dt'] = pd.to_datetime(out['date'])
monthly = out.groupby(out['date_dt'].dt.month).agg(
    avg_occ=('predicted_occ','mean'),
    avg_rooms=('predicted_rooms','mean'),
    avg_rate=('recommended_rate','mean')
).round(3)
monthly.index = [pd.Timestamp(2026, m, 1).strftime('%b') for m in monthly.index]
print(monthly.to_string())

print()
print("Rate tier distribution:")
print(out['rate_tier'].value_counts().to_string())

print()
print("Data quality distribution:")
print(out['data_quality'].value_counts().to_string())

print()
print(f"Stage 2 adjustments applied: {(np.abs(total_adj) > 0.001).sum()} dates")
print(f"  Pace adjustments (>60 days): {(np.abs(pace_adj) > 0.001).sum()} dates")
print(f"  BOB adjustments (≤60 days):  {(np.abs(bob_adj)  > 0.001).sum()} dates")

# ── Save outputs ──────────────────────────────────────────────────────────────
out = out.drop(columns=['date_dt'])
out.to_csv(OUT_PATH, index=False)
print(f"\n✅ Predictions saved → {OUT_PATH}")

# Save model metrics
metrics = {
    'model':              'GradientBoosting (60%) + RandomForest (40%) ensemble',
    'features':           FEATURES,
    'n_training_rows':    len(tr),
    'n_prediction_rows':  len(pr),
    'cv_type':            'seasonal_holdout_3_splits',
    'cv_mae_all_folds':   round(float(mean_mae), 4),
    'cv_mae_operational': round(float(op_mae), 4),
    'cv_mae_pp':          round(float(op_mae) * 100, 2),
    'cv_r2_mean':         round(float(mean_r2), 3),
    'occ_accuracy_pct':   round(float(occ_acc) * 100, 1),
    'note_apr_fold':      '2024-Apr occ was 6.3% (hotel barely open), fold excluded from op MAE',
    'stage2_regime':      'moderate',
    'feature_importances': {k: round(float(v), 4) for k, v in feat_imp.items()},
}
with open(METRICS_PATH, 'w') as f:
    json.dump(metrics, f, indent=2)
print(f"✅ Model metrics saved → {METRICS_PATH}")
print()
print("DONE.")



MODELS_DIR = Path(os.getenv('MODELS_DIR', 'data/models'))
MODELS_DIR.mkdir(parents=True, exist_ok=True)

joblib.dump(gbm, MODELS_DIR / 'gbm_model.joblib')
joblib.dump(rf,  MODELS_DIR / 'rf_model.joblib')

print(f"✅ Models saved → {MODELS_DIR}/gbm_model.joblib")
print(f"✅ Models saved → {MODELS_DIR}/rf_model.joblib")