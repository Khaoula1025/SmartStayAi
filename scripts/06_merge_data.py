"""
SmartStay Intelligence — Hickstead Hotel
Script 06: Build Training Matrix

Inputs (all clean files from scripts 01–05):
  - clean_occupancy.csv
  - clean_day_by_day.csv
  - clean_bookingcom.csv
  - clean_floor_by_date.csv
  - clean_pickup.csv

Output:
  - training_matrix.csv   — historical rows (2024+2025) for ML training
  - prediction_matrix.csv — future rows (2026) for ML prediction

Merge strategy:
  ┌─────────────────────────────────────────────────────────────┐
  │ ZONE A — 2025 training (364 rows)                          │
  │   occupancy (actuals) + bookingcom (rates) + fit (floor)   │
  │   + day_by_day joined on date_2025 → cs_occ, cs_adr        │
  ├─────────────────────────────────────────────────────────────┤
  │ ZONE B — 2024 training (274 rows)                          │
  │   occupancy (actuals) + fit (floor)                        │
  │   bookingcom = NaN (file only covers 2025)                 │
  │   cs_occ/cs_adr filled from DOW medians                   │
  ├─────────────────────────────────────────────────────────────┤
  │ ZONE C — 2026 prediction (307 rows)                        │
  │   pickup (BOB) + day_by_day (budget) + fit (floor)         │
  │   target (occ_rate) = NaN — this is what model predicts    │
  └─────────────────────────────────────────────────────────────┘

Run:
  python 06_build_matrix.py
"""

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
CLEAN_DIR = 'data/processed'

OUT_TRAINING   = 'data/processed/training_matrix.csv'
OUT_PREDICTION = 'data/processed/prediction_matrix.csv'

print("=" * 60)
print("SmartStay — Script 06: Build Training & Prediction Matrix")
print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD ALL CLEAN FILES
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/8] Loading clean files...")

occ = pd.read_csv(f'{CLEAN_DIR}/clean_occupancy.csv')
dbd = pd.read_csv(f'{CLEAN_DIR}/clean_day_by_day.csv')
bco = pd.read_csv(f'{CLEAN_DIR}/clean_bookingcom.csv')
fit = pd.read_csv(f'{CLEAN_DIR}/clean_floor_by_date.csv')
pu  = pd.read_csv(f'{CLEAN_DIR}/clean_pickup.csv')

occ['date']       = pd.to_datetime(occ['date'])
dbd['date_2026']  = pd.to_datetime(dbd['date_2026'])
dbd['date_2025']  = pd.to_datetime(dbd['date_2025'])
bco['date']       = pd.to_datetime(bco['date'])
fit['date']       = pd.to_datetime(fit['date'])
pu['date']        = pd.to_datetime(pu['date'])

print(f"      occupancy:  {len(occ)} rows")
print(f"      day_by_day: {len(dbd)} rows")
print(f"      bookingcom: {len(bco)} rows")
print(f"      fit:        {len(fit)} rows")
print(f"      pickup:     {len(pu)} rows")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — PREPARE BOOKINGCOM — select columns needed
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/8] Preparing source columns...")

bco_cols = bco[['date', 'own_rate', 'compset_median', 'price_rank',
                'bcom_rank', 'bcom_rank_norm', 'is_bank_holiday',
                'is_cultural_holiday']].copy()

# From day_by_day: bring cs_occ, cs_adr keyed on date_2025
# (these are the 2025 comp set actuals mapped from actual 2025 dates)
dbd_for_2025 = dbd[['date_2025', 'cs_occ', 'cs_adr', 'cs_revpar_2025', 'h_adr']].copy()
dbd_for_2025 = dbd_for_2025.rename(columns={'date_2025': 'date'})

# From day_by_day: bring 2026 budget + cs columns keyed on date_2026
dbd_for_2026 = dbd[['date_2026', 'cs_occ', 'cs_adr', 'cs_revpar_2025',
                     'b_occ', 'b_adr', 'b_rns', 'b_rev',
                     'budget_occ_gap', 'budget_adr_gap']].copy()
dbd_for_2026 = dbd_for_2026.rename(columns={'date_2026': 'date'})

# FIT: just floor_price and season
fit_cols = fit[['date', 'floor_price', 'season']].copy()

print("      Column sets prepared ✅")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — BUILD ZONE A: 2025 TRAINING ROWS
# Base: occupancy 2025
# Add: bookingcom (own_rate, compset_median, ranks, holidays)
# Add: day_by_day cs_occ/cs_adr via date_2025 match
# Add: fit floor_price
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/8] Building Zone A — 2025 training rows...")

zone_a = occ[occ['date'].dt.year == 2025].copy()
zone_a = zone_a.merge(bco_cols,    on='date', how='left')
zone_a = zone_a.merge(dbd_for_2025, on='date', how='left')
zone_a = zone_a.merge(fit_cols,    on='date', how='left')
zone_a['data_zone'] = 'A_2025_training'

print(f"      {len(zone_a)} rows | features from: occupancy + bookingcom + day_by_day + fit")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — BUILD ZONE B: 2024 TRAINING ROWS
# Base: occupancy 2024
# Add: fit floor_price
# bookingcom = not available for 2024
# cs_occ / cs_adr = fill from day_by_day DOW+month medians
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/8] Building Zone B — 2024 training rows...")

zone_b = occ[occ['date'].dt.year == 2024].copy()
zone_b = zone_b.merge(fit_cols, on='date', how='left')
zone_b['data_zone'] = 'B_2024_training'

# Add placeholder columns that exist in zone_a so concat works cleanly
for col in ['own_rate', 'compset_median', 'price_rank', 'bcom_rank',
            'bcom_rank_norm', 'is_bank_holiday', 'is_cultural_holiday']:
    zone_b[col] = np.nan

# Fill cs_occ / cs_adr for 2024 using DOW medians from 2025 data
# (best available proxy — same hotel, same market, one year earlier)
# NOTE: occupancy uses 3-letter DOW ('Mon') — normalise to match
cs_medians = dbd_for_2025.copy()
cs_medians['dow_abbr'] = pd.to_datetime(cs_medians['date']).dt.strftime('%a')  # 'Mon','Tue'...
cs_dow_medians = cs_medians.groupby('dow_abbr')[['cs_occ','cs_adr','cs_revpar_2025']].median()

def fill_cs(row, col):
    if pd.notna(row.get(col)):
        return row[col]
    dow = row['dow']  # already 3-letter abbreviation
    return cs_dow_medians.loc[dow, col] if dow in cs_dow_medians.index else np.nan

zone_b['cs_occ']         = zone_b.apply(lambda r: fill_cs(r, 'cs_occ'), axis=1)
zone_b['cs_adr']         = zone_b.apply(lambda r: fill_cs(r, 'cs_adr'), axis=1)
zone_b['cs_revpar_2025'] = zone_b.apply(lambda r: fill_cs(r, 'cs_revpar_2025'), axis=1)

# is_bank_holiday for 2024 — hardcode England & Wales bank holidays
ENGLAND_BH_2024 = pd.to_datetime([
    '2024-01-01', '2024-03-29', '2024-04-01',
    '2024-05-06', '2024-05-27', '2024-08-26',
    '2024-12-25', '2024-12-26',
])
CULTURAL_2024 = pd.to_datetime([
    '2024-02-14', '2024-03-10', '2024-06-16',  # Valentine's, Mother's, Father's
])
zone_b['is_bank_holiday']     = zone_b['date'].isin(ENGLAND_BH_2024).astype(int)
zone_b['is_cultural_holiday'] = zone_b['date'].isin(CULTURAL_2024).astype(int)

# Budget columns don't exist for 2024
for col in ['b_occ','b_adr','b_rns','b_rev','budget_occ_gap','budget_adr_gap']:
    zone_b[col] = np.nan

print(f"      {len(zone_b)} rows | features from: occupancy + fit + imputed cs_occ/cs_adr")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — COMBINE TRAINING ZONES A + B
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/8] Combining training zones...")
training = pd.concat([zone_b, zone_a], ignore_index=True)
training = training.sort_values('date').reset_index(drop=True)

print(f"      {len(training)} total training rows")
print(f"      Zone A (2025): {(training['data_zone']=='A_2025_training').sum()}")
print(f"      Zone B (2024): {(training['data_zone']=='B_2024_training').sum()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — BUILD ZONE C: 2026 PREDICTION ROWS
# Base: pickup BOB
# Add: day_by_day (cs_occ, cs_adr, budget targets)
# Add: fit floor_price
# occ_rate = NaN (this is what the model predicts)
# ─────────────────────────────────────────────────────────────────────────────

print("\n[6/8] Building Zone C — 2026 prediction rows...")

pu_cols = pu[['date', 'dow', 'bob_sold', 'bob_occ', 'bob_adr',
              'pickup_1d', 'pickup_7d', 'pickup_velocity',
              'stly_sold', 'available_rooms', 'days_ahead']].copy()

# Build calendar features for 2026
pu_cols['year']          = pu_cols['date'].dt.year
pu_cols['month']         = pu_cols['date'].dt.month
pu_cols['day_of_week']   = pu_cols['date'].dt.dayofweek
pu_cols['week_of_year']  = pu_cols['date'].dt.isocalendar().week.astype(int)
pu_cols['is_weekend']    = pu_cols['date'].dt.dayofweek.isin([4,5,6]).astype(int)
pu_cols['is_high_season']= pu_cols['month'].isin([4,5,6,7,9,10,11,12]).astype(int)

# Bank + cultural holidays for 2026
ENGLAND_BH_2026 = pd.to_datetime([
    '2026-01-01', '2026-04-03', '2026-04-06',
    '2026-05-04', '2026-05-25', '2026-08-31',
    '2026-12-25', '2026-12-28',
])
CULTURAL_2026 = pd.to_datetime([
    '2026-02-14', '2026-03-15', '2026-06-21',  # Valentine's, Mother's, Father's
])
pu_cols['is_bank_holiday']     = pu_cols['date'].isin(ENGLAND_BH_2026).astype(int)
pu_cols['is_cultural_holiday'] = pu_cols['date'].isin(CULTURAL_2026).astype(int)

# Merge day_by_day and fit
prediction = pu_cols.merge(dbd_for_2026, on='date', how='left')
prediction = prediction.merge(fit_cols,  on='date', how='left')

# Target column is unknown — model will fill this
prediction['occ_rate']  = np.nan
prediction['data_zone'] = 'C_2026_prediction'

print(f"      {len(prediction)} rows | features from: pickup + day_by_day + fit")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — ENGINEER FINAL FEATURES ON TRAINING DATA
# Add lag features and rolling averages needed by the model.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[7/8] Engineering lag features on training data...")

# Drop rows with no target — can't train without occ_rate
before = len(training)
training = training.dropna(subset=['occ_rate']).reset_index(drop=True)
print(f"      Dropped {before - len(training)} rows with missing occ_rate (PMS gaps)")

# Fill floor_price for 2024 dates (FIT contract starts Feb 2025 — use low season as floor)
# Low season DOW rates for Double Room: Mon-Thu £85, Fri-Sat £79, Sun £75
LOW_SEASON_DOW = {'Mon':85,'Tue':85,'Wed':85,'Thu':85,'Fri':79,'Sat':79,'Sun':75}
missing_fp = training['floor_price'].isna()
training.loc[missing_fp, 'floor_price'] = training.loc[missing_fp, 'dow'].map(LOW_SEASON_DOW)
training.loc[missing_fp, 'season'] = 'low'
print(f"      Filled {missing_fp.sum()} missing floor_price values for 2024 rows")

training = training.sort_values('date').reset_index(drop=True)

# Lag occupancy: what was occupancy 7 and 28 days ago?
training['occ_lag_7']  = training['occ_rate'].shift(7)
training['occ_lag_28'] = training['occ_rate'].shift(28)

# Rolling 7-day average occupancy
training['occ_roll7'] = training['occ_rate'].rolling(7, min_periods=3).mean()

# Price position vs compset (use own_rate from bookingcom where available)
# For 2024 where own_rate is NaN, fall back to h_adr from day_by_day
training['own_rate_filled'] = training['own_rate'].fillna(training.get('h_adr', np.nan))
training['price_pos'] = (training['own_rate_filled'] / training['cs_adr']).round(3)

print(f"      Added: occ_lag_7, occ_lag_28, occ_roll7, price_pos")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — SAVE BOTH MATRICES
# ─────────────────────────────────────────────────────────────────────────────

print("\n[8/8] Saving matrices...")

# Final column order for training matrix
training_cols = [
    # Identity
    'date', 'data_zone', 'year', 'month', 'dow', 'day_of_week',
    'week_of_year', 'is_weekend', 'is_high_season',
    # Target
    'occ_rate',
    # Occupancy features
    'rooms_let', 'total_rooms', 'occ_lag_7', 'occ_lag_28', 'occ_roll7',
    # Pricing features
    'own_rate', 'own_rate_filled', 'compset_median', 'cs_occ', 'cs_adr',
    'price_rank', 'price_pos', 'floor_price', 'season',
    # Booking.com signals
    'bcom_rank', 'bcom_rank_norm', 'is_bank_holiday', 'is_cultural_holiday',
    # Budget/targets
    'b_occ', 'b_adr',
]
# Keep only columns that exist
training_cols = [c for c in training_cols if c in training.columns]
training[training_cols].to_csv(OUT_TRAINING, index=False)

# Prediction matrix
pred_cols = [
    'date', 'data_zone', 'year', 'month', 'dow', 'day_of_week',
    'week_of_year', 'is_weekend', 'is_high_season',
    'occ_rate',  # NaN — to be filled by model
    'bob_sold', 'bob_occ', 'bob_adr', 'pickup_1d', 'pickup_7d',
    'pickup_velocity', 'stly_sold', 'available_rooms', 'days_ahead',
    'cs_occ', 'cs_adr', 'floor_price', 'season',
    'b_occ', 'b_adr', 'b_rns', 'b_rev',
    'is_bank_holiday', 'is_cultural_holiday',
]
pred_cols = [c for c in pred_cols if c in prediction.columns]
prediction[pred_cols].to_csv(OUT_PREDICTION, index=False)


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("✅  DONE")
print("=" * 60)
print(f"  Training matrix:   {OUT_TRAINING}")
print(f"    Rows:    {len(training)}")
print(f"    Columns: {len(training_cols)}")
print(f"    Target (occ_rate) missing: {training['occ_rate'].isna().sum()} rows")
print()
print(f"  Prediction matrix: {OUT_PREDICTION}")
print(f"    Rows:    {len(prediction)}")
print(f"    Columns: {len(pred_cols)}")
print()

tm = pd.read_csv(OUT_TRAINING)
print("  Training matrix — missing value report:")
key_cols = ['occ_rate','compset_median','cs_occ','cs_adr',
            'own_rate','floor_price','is_bank_holiday']
for col in key_cols:
    if col in tm.columns:
        miss = tm[col].isna().sum()
        pct  = miss / len(tm) * 100
        flag = ' ⚠️' if pct > 20 else ''
        print(f"    {col:<25} {miss:>4} missing ({pct:.0f}%){flag}")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("🔍  VALIDATION")
print("=" * 60)

checks_passed = 0
checks_failed = 0

def check(name, condition, detail=""):
    global checks_passed, checks_failed
    if condition:
        print(f"  ✅  PASS  {name}")
        checks_passed += 1
    else:
        print(f"  ❌  FAIL  {name}")
        if detail:
            print(f"            → {detail}")
        checks_failed += 1

tr = pd.read_csv(OUT_TRAINING,   parse_dates=['date'])
pr = pd.read_csv(OUT_PREDICTION, parse_dates=['date'])

# Training checks
check("Training has 400+ rows", len(tr) >= 400, f"Got {len(tr)}")
check("No duplicate dates in training", tr['date'].duplicated().sum() == 0,
      f"{tr['date'].duplicated().sum()} duplicates")
check("occ_rate has no missing values in training",
      tr['occ_rate'].isna().sum() == 0,
      f"{tr['occ_rate'].isna().sum()} missing — should be 0 after dropping gap rows")
check("occ_rate all between 0 and 1",
      ((tr['occ_rate'].dropna() >= 0) & (tr['occ_rate'].dropna() <= 1)).all(),
      "Values outside 0–1 range")
check("floor_price present for most training rows",
      tr['floor_price'].notna().mean() > 0.5,
      f"Only {tr['floor_price'].notna().mean():.0%} coverage")
check("cs_occ present for most training rows",
      tr['cs_occ'].notna().mean() > 0.5,
      f"Only {tr['cs_occ'].notna().mean():.0%} coverage")
check("is_bank_holiday present and binary",
      tr['is_bank_holiday'].isin([0, 1]).all(),
      f"Values: {tr['is_bank_holiday'].unique()}")

# Prediction checks
check("Prediction has 300+ rows", len(pr) >= 300, f"Got {len(pr)}")
check("All prediction dates in 2026",
      (pr['date'].dt.year == 2026).all(),
      f"Non-2026 dates: {pr[pr['date'].dt.year != 2026]['date'].tolist()[:3]}")
check("occ_rate is NaN in prediction (target unknown)",
      pr['occ_rate'].isna().all(),
      f"{pr['occ_rate'].notna().sum()} non-null rows in prediction")
check("pickup features present in prediction",
      'bob_sold' in pr.columns and pr['bob_sold'].notna().sum() > 200,
      "bob_sold missing or mostly null")
check("floor_price present in prediction",
      pr['floor_price'].notna().mean() > 0.9,
      f"Only {pr['floor_price'].notna().mean():.0%} coverage")

print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 Matrices ready — next step: train the XGBoost model")
else:
    print("  ⚠️  Fix the failed checks before training")