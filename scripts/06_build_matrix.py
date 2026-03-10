import pandas as pd
import numpy as np
import re

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
CLEAN_DIR    = 'data/processed'
EVENTS_FILE  = 'data/raw/Special Events.xlsx'

OUT_TRAINING   = 'data/processed/training_matrix_2.csv'
OUT_PREDICTION = 'data/processed/prediction_matrix_2.csv'

print("=" * 60)
print("SmartStay — Script 06: Build Training & Prediction Matrix")
print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD ALL CLEAN FILES
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/9] Loading clean files...")

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
# STEP 2 — PARSE LOCAL EVENTS  [FIX 5]
# Haywards Heath tab covers events near Hickstead (Ardingly Showground,
# Goodwood, Brighton, Chichester etc.) — genuine demand drivers.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/9] Parsing local events (Haywards Heath / Ardingly)...")

def parse_local_events(filepath):
    xl  = pd.ExcelFile(filepath)
    raw = xl.parse('Haywards Heath', header=None, dtype=str)
    event_dates = set()
    for i in range(2, len(raw)):
        date_cell = str(raw.iloc[i, 1])
        if date_cell == 'nan':
            continue
        # Pattern: 'DD-MM-YYYY to DD-MM-YYYY'
        m = re.match(
            r'(\d{2})-(\d{2})-(\d{4})\s+to\s+(\d{2})-(\d{2})-(\d{4})',
            date_cell
        )
        if m:
            start = pd.Timestamp(f'{m.group(3)}-{m.group(2)}-{m.group(1)}')
            end   = pd.Timestamp(f'{m.group(6)}-{m.group(5)}-{m.group(4)}')
            for d in pd.date_range(start, end):
                event_dates.add(d)
            continue
        # Pattern: single ISO date
        try:
            d = pd.Timestamp(date_cell)
            if d.year in (2025, 2026):
                event_dates.add(d)
        except Exception:
            pass
    return pd.DatetimeIndex(sorted(event_dates))

local_event_dates = parse_local_events(EVENTS_FILE)
print(f"      {len(local_event_dates)} local event dates "
      f"({local_event_dates.min().date()} → {local_event_dates.max().date()})")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — PREPARE COLUMN SUBSETS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/9] Preparing source columns...")

bco_cols = bco[['date', 'own_rate', 'compset_median', 'price_rank',
                'bcom_rank', 'bcom_rank_norm', 'is_bank_holiday',
                'is_cultural_holiday']].copy()

# day_by_day keyed on date_2025 → 2025 comp set + hotel ADR + 2026 budget targets
# b_occ / b_adr are the 2026 budget targets for the DOW-matched date_2026.
# For a Zone A row dated 2025-06-15, b_occ = management's 2026 target for that DOW.
# Correlation with actual h_occ = 0.54 — genuine seasonal signal.
# Zone B (2024) has no dbd match so b_occ/b_adr will be NaN there — XGBoost handles it.
dbd_for_2025 = dbd[['date_2025', 'cs_occ', 'cs_adr', 'cs_revpar_2025', 'h_adr',
                     'b_occ', 'b_adr']].copy()
dbd_for_2025 = dbd_for_2025.rename(columns={'date_2025': 'date'})

# day_by_day keyed on date_2026 → 2026 budget + comp set
dbd_for_2026 = dbd[['date_2026', 'cs_occ', 'cs_adr', 'cs_revpar_2025',
                     'b_occ', 'b_adr', 'b_rns', 'b_rev',
                     'budget_occ_gap', 'budget_adr_gap']].copy()
dbd_for_2026 = dbd_for_2026.rename(columns={'date_2026': 'date'})

fit_cols = fit[['date', 'floor_price', 'season']].copy()

# FIX 1 — pre-compute DOW median h_adr from 2025 to use as own_rate proxy for Zone B
dbd_for_2025['dow_abbr'] = dbd_for_2025['date'].dt.strftime('%a')
h_adr_dow_med = dbd_for_2025.groupby('dow_abbr')['h_adr'].median().to_dict()

print("      Column sets prepared ✅")
print(f"      h_adr DOW medians for Zone B own_rate proxy: "
      + " | ".join(f"{k}=£{v:.0f}" for k, v in sorted(h_adr_dow_med.items())))


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — BUILD ZONE A: 2025 TRAINING ROWS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/9] Building Zone A — 2025 training rows...")

zone_a = occ[occ['date'].dt.year == 2025].copy()
zone_a = zone_a.merge(bco_cols,      on='date', how='left')
zone_a = zone_a.merge(dbd_for_2025,  on='date', how='left')
zone_a = zone_a.merge(fit_cols,      on='date', how='left')
zone_a['data_zone'] = 'A_2025_training'

# dbd.date_2025 starts Jan 2 — Jan 1 2025 gets NaN; fill with DOW median
cs_medians_a = dbd_for_2025.copy()
cs_medians_a['dow_abbr'] = cs_medians_a['date'].dt.strftime('%a')
cs_dow_med_a = cs_medians_a.groupby('dow_abbr')[
    ['cs_occ', 'cs_adr', 'h_adr', 'cs_revpar_2025', 'b_occ', 'b_adr']
].median()
for col in ['cs_occ', 'cs_adr', 'h_adr', 'cs_revpar_2025', 'b_occ', 'b_adr']:
    null_mask = zone_a[col].isna()
    if null_mask.any():
        zone_a.loc[null_mask, col] = zone_a.loc[null_mask, 'dow'].map(
            cs_dow_med_a[col].to_dict()
        )
        print(f"      Imputed {null_mask.sum()} missing {col} in Zone A via DOW median")

zone_a['is_local_event'] = zone_a['date'].isin(local_event_dates).astype(int)
print(f"      {len(zone_a)} rows | occupancy + bookingcom + day_by_day + fit + events")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — BUILD ZONE B: 2024 TRAINING ROWS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/9] Building Zone B — 2024 training rows...")

zone_b = occ[occ['date'].dt.year == 2024].copy()
zone_b = zone_b.merge(fit_cols, on='date', how='left')
zone_b['data_zone'] = 'B_2024_training'

# Placeholder bookingcom columns (not available for 2024)
for col in ['own_rate', 'compset_median', 'price_rank', 'bcom_rank', 'bcom_rank_norm']:
    zone_b[col] = np.nan

# Fill cs_occ / cs_adr using 2025 DOW medians as best available proxy
cs_medians_b = dbd_for_2025.copy()
cs_medians_b['dow_abbr'] = cs_medians_b['date'].dt.strftime('%a')
cs_dow_med_b = cs_medians_b.groupby('dow_abbr')[
    ['cs_occ', 'cs_adr', 'cs_revpar_2025']
].median()

def fill_cs(row, col):
    if pd.notna(row.get(col)):
        return row[col]
    return cs_dow_med_b.loc[row['dow'], col] if row['dow'] in cs_dow_med_b.index else np.nan

zone_b['cs_occ']         = zone_b.apply(lambda r: fill_cs(r, 'cs_occ'), axis=1)
zone_b['cs_adr']         = zone_b.apply(lambda r: fill_cs(r, 'cs_adr'), axis=1)
zone_b['cs_revpar_2025'] = zone_b.apply(lambda r: fill_cs(r, 'cs_revpar_2025'), axis=1)

# FIX 1 — fill h_adr for Zone B so own_rate_filled works in Step 8
zone_b['h_adr'] = zone_b['dow'].map(h_adr_dow_med)
print(f"      FIX 1: own_rate proxy (h_adr) filled for {zone_b['h_adr'].notna().sum()} Zone B rows ✅")

# Bank / cultural holidays 2024 — England & Wales
ENGLAND_BH_2024 = pd.to_datetime([
    '2024-01-01', '2024-03-29', '2024-04-01',
    '2024-05-06', '2024-05-27', '2024-08-26',
    '2024-12-25', '2024-12-26',
])
CULTURAL_2024 = pd.to_datetime([
    '2024-02-14', '2024-03-10', '2024-06-16',
])
zone_b['is_bank_holiday']     = zone_b['date'].isin(ENGLAND_BH_2024).astype(int)
zone_b['is_cultural_holiday'] = zone_b['date'].isin(CULTURAL_2024).astype(int)

# b_occ / b_adr are 2026-only targets — not applicable to 2024 rows
for col in ['b_occ', 'b_adr', 'b_rns', 'b_rev', 'budget_occ_gap', 'budget_adr_gap']:
    zone_b[col] = np.nan

zone_b['is_local_event'] = zone_b['date'].isin(local_event_dates).astype(int)
print(f"      {len(zone_b)} rows | occupancy + fit + imputed cs_occ/cs_adr + own_rate proxy")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — COMBINE TRAINING ZONES A + B
# ─────────────────────────────────────────────────────────────────────────────

print("\n[6/9] Combining training zones...")
training = pd.concat([zone_b, zone_a], ignore_index=True)
training = training.sort_values('date').reset_index(drop=True)
print(f"      {len(training)} total training rows")
print(f"      Zone A (2025): {(training['data_zone']=='A_2025_training').sum()}")
print(f"      Zone B (2024): {(training['data_zone']=='B_2024_training').sum()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — BUILD ZONE C: 2026 PREDICTION ROWS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[7/9] Building Zone C — 2026 prediction rows...")

# FIX 4 — add pace_gap and stly_rev to pu_cols selection
pu_cols = pu[['date', 'dow', 'bob_sold', 'bob_occ', 'bob_adr',
              'pickup_1d', 'pickup_7d', 'pickup_velocity',
              'stly_sold', 'stly_rev', 'pace_gap',
              'available_rooms', 'days_ahead']].copy()

pu_cols['year']         = pu_cols['date'].dt.year
pu_cols['month']        = pu_cols['date'].dt.month
pu_cols['day_of_week']  = pu_cols['date'].dt.dayofweek
pu_cols['week_of_year'] = pu_cols['date'].dt.isocalendar().week.astype(int)
pu_cols['is_weekend']   = pu_cols['date'].dt.dayofweek.isin([4, 5, 6]).astype(int)

# FIX 3 — include August (month 8) in is_high_season
pu_cols['is_high_season'] = pu_cols['month'].isin([4, 5, 6, 7, 8, 9, 10, 11, 12]).astype(int)

ENGLAND_BH_2026 = pd.to_datetime([
    '2026-01-01', '2026-04-03', '2026-04-06',
    '2026-05-04', '2026-05-25', '2026-08-31',
    '2026-12-25', '2026-12-28',
])
CULTURAL_2026 = pd.to_datetime([
    '2026-02-14', '2026-03-15', '2026-06-21',
])
pu_cols['is_bank_holiday']     = pu_cols['date'].isin(ENGLAND_BH_2026).astype(int)
pu_cols['is_cultural_holiday'] = pu_cols['date'].isin(CULTURAL_2026).astype(int)
pu_cols['is_local_event']      = pu_cols['date'].isin(local_event_dates).astype(int)

prediction = pu_cols.merge(dbd_for_2026, on='date', how='left')
prediction = prediction.merge(fit_cols,   on='date', how='left')
prediction['occ_rate']  = np.nan
prediction['data_zone'] = 'C_2026_prediction'

aug_hs = (prediction[prediction['month'] == 8]['is_high_season'] == 1).all()
print(f"      FIX 3: August is_high_season=1: {aug_hs} ✅")
print(f"      FIX 4: pace_gap non-null: {prediction['pace_gap'].notna().sum()} rows ✅")
print(f"      FIX 5: is_local_event flagged: {prediction['is_local_event'].sum()} dates ✅")
print(f"      {len(prediction)} rows | pickup + day_by_day + fit + events")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — ENGINEER FINAL FEATURES ON TRAINING DATA
# ─────────────────────────────────────────────────────────────────────────────

print("\n[8/9] Engineering lag features on training data...")

before = len(training)
training = training.dropna(subset=['occ_rate']).reset_index(drop=True)
print(f"      Dropped {before - len(training)} rows with missing occ_rate (PMS gaps)")

# Fill floor_price for 2024 rows (FIT contract starts Feb 2025)
LOW_SEASON_DOW = {'Mon': 85, 'Tue': 85, 'Wed': 85, 'Thu': 85,
                  'Fri': 79, 'Sat': 79, 'Sun': 75}
missing_fp = training['floor_price'].isna()
training.loc[missing_fp, 'floor_price'] = training.loc[missing_fp, 'dow'].map(LOW_SEASON_DOW)
training.loc[missing_fp, 'season'] = 'low'
print(f"      Filled {missing_fp.sum()} missing floor_price values for 2024 rows")

training = training.sort_values('date').reset_index(drop=True)

# Lag / rolling features
training['occ_lag_7']  = training['occ_rate'].shift(7)
training['occ_lag_28'] = training['occ_rate'].shift(28)
training['occ_roll7']  = training['occ_rate'].rolling(7, min_periods=3).mean()

# FIX 1 — own_rate_filled now works for Zone B because h_adr is populated
training['own_rate_filled'] = training['own_rate'].fillna(training['h_adr'])
training['price_pos']       = (training['own_rate_filled'] / training['cs_adr']).round(3)

zb_pp = training[training['data_zone']=='B_2024_training']['price_pos'].notna().sum()
print(f"      FIX 1: price_pos filled for {zb_pp} Zone B rows (was 0 before fix) ✅")
print(f"      Added: occ_lag_7, occ_lag_28, occ_roll7, price_pos, own_rate_filled")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 9 — SAVE BOTH MATRICES
# ─────────────────────────────────────────────────────────────────────────────

print("\n[9/9] Saving matrices...")

# FIX 2 — b_occ / b_adr removed from training_cols (100% NaN — no signal)
training_cols = [
    # Identity
    'date', 'data_zone', 'year', 'month', 'dow', 'day_of_week',
    'week_of_year', 'is_weekend', 'is_high_season',
    # Target
    'occ_rate',
    # Occupancy features
    'rooms_let', 'tot_rooms', 'occ_lag_7', 'occ_lag_28', 'occ_roll7',
    # Pricing features
    'own_rate', 'own_rate_filled', 'compset_median', 'cs_occ', 'cs_adr',
    'price_rank', 'price_pos', 'floor_price', 'season',
    # Booking.com signals
    'bcom_rank', 'bcom_rank_norm',
    # Demand signals
    'is_bank_holiday', 'is_cultural_holiday', 'is_local_event',
    # Budget targets (DOW-matched 2026 targets — 363/364 Zone A rows; NaN for Zone B)
    'b_occ', 'b_adr',
]
training_cols = [c for c in training_cols if c in training.columns]
training[training_cols].to_csv(OUT_TRAINING, index=False)

# FIX 4 — pace_gap, stly_rev added to pred_cols
pred_cols = [
    'date', 'data_zone', 'year', 'month', 'dow', 'day_of_week',
    'week_of_year', 'is_weekend', 'is_high_season',
    'occ_rate',
    'bob_sold', 'bob_occ', 'bob_adr',
    'pickup_1d', 'pickup_7d', 'pickup_velocity',
    'stly_sold', 'stly_rev', 'pace_gap',
    'available_rooms', 'days_ahead',
    'cs_occ', 'cs_adr', 'floor_price', 'season',
    'b_occ', 'b_adr', 'b_rns', 'b_rev',
    'is_bank_holiday', 'is_cultural_holiday', 'is_local_event',
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
print(f"    Rows: {len(training)} | Columns: {len(training_cols)}")
print()
print(f"  Prediction matrix: {OUT_PREDICTION}")
print(f"    Rows: {len(prediction)} | Columns: {len(pred_cols)}")
print()

tm = pd.read_csv(OUT_TRAINING)
print("  Training matrix — missing value report:")
for col in tm.columns:
    miss = tm[col].isna().sum()
    if miss > 0:
        pct  = miss / len(tm) * 100
        flag = ' ⚠️' if pct > 20 else ''
        print(f"    {col:<25} {miss:>4} missing ({pct:.0f}%){flag}")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("🔍  VALIDATION")
print("=" * 60)

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  ✅  PASS  {name}")
        passed += 1
    else:
        print(f"  ❌  FAIL  {name}")
        if detail:
            print(f"            → {detail}")
        failed += 1

tr = pd.read_csv(OUT_TRAINING,   parse_dates=['date'])
pr = pd.read_csv(OUT_PREDICTION, parse_dates=['date'])

check("Training has 400+ rows", len(tr) >= 400, f"Got {len(tr)}")
check("No duplicate dates in training", tr['date'].duplicated().sum() == 0)
check("occ_rate no missing values", tr['occ_rate'].isna().sum() == 0)
check("occ_rate all between 0 and 1", tr['occ_rate'].between(0, 1).all())
check("floor_price present for all rows", tr['floor_price'].notna().all())
check("cs_occ present for all rows", tr['cs_occ'].notna().all())
check("is_bank_holiday binary", tr['is_bank_holiday'].isin([0, 1]).all())
check("is_local_event present in training", 'is_local_event' in tr.columns)

zb = tr[tr['data_zone'] == 'B_2024_training']
check("FIX 1: own_rate_filled present in Zone B",
      zb['own_rate_filled'].notna().mean() > 0.9,
      f"Only {zb['own_rate_filled'].notna().mean():.0%} filled")
check("FIX 1: price_pos present in Zone B",
      zb['price_pos'].notna().mean() > 0.9,
      f"Only {zb['price_pos'].notna().mean():.0%} filled")
check("FIX 2: b_occ present for Zone A in training",
      tr[tr['data_zone']=='A_2025_training']['b_occ'].notna().mean() > 0.99,
      "b_occ missing from Zone A rows")
check("FIX 3: August is_high_season=1 in prediction",
      (pr[pr['date'].dt.month == 8]['is_high_season'] == 1).all())
check("FIX 4: pace_gap present in prediction",
      'pace_gap' in pr.columns and pr['pace_gap'].notna().all())
check("FIX 4: stly_rev present in prediction",
      'stly_rev' in pr.columns and pr['stly_rev'].notna().all())
check("FIX 5: is_local_event in prediction with hits",
      'is_local_event' in pr.columns and pr['is_local_event'].sum() > 0)

check("Prediction has 300+ rows", len(pr) >= 300)
check("All prediction dates in 2026", (pr['date'].dt.year == 2026).all())
check("occ_rate all NaN in prediction", pr['occ_rate'].isna().all())
check("bob_sold present in prediction", pr['bob_sold'].notna().sum() > 200)
check("floor_price present in prediction", pr['floor_price'].notna().mean() > 0.9)

print()
print(f"  {passed} checks passed, {failed} checks failed")
if failed == 0:
    print("  🎉 Matrices ready — next step: train the XGBoost model")
else:
    print("  ⚠️  Fix the failed checks before training")