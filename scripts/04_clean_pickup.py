import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FILE_BOB   = 'data/raw/Uno Hotels Pickup. 27.02.2026.xlsx'
FILE_STLY  = 'data/raw/Uno Hotels Pickup 18.12.25.xlsx'
OUTPUT     = 'data/processed/clean_pickup_1.csv'
HOTEL_CAP  = 52
EXTRACT_DATE = pd.Timestamp('2026-02-27')
HOTEL_NAME   = 'The Hickstead Hotel by Uno'

# Column positions in Daily Pick-Up sheet (0-indexed)
COL_DATE        = 2
COL_DOW         = 3
COL_MONTH       = 4
COL_CAP         = 5
COL_OOO         = 6
COL_BOB_SOLD    = 7
COL_BOB_OCC     = 8
COL_BOB_REV     = 9
COL_BOB_ADR     = 10
COL_PICKUP_SOLD = 20   # 1-day pickup (rooms sold since yesterday)

print("=" * 58)
print("SmartStay — Script 04: Clean Pickup / BOB Data")
print("=" * 58)
print(f"  Extract date : {EXTRACT_DATE.date()}")
print(f"  BOB file     : {FILE_BOB.split('/')[-1]}")
print(f"  STLY file    : {FILE_STLY.split('/')[-1]}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PARSE DAILY PICK-UP (BOB file)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[1/7] Parsing Daily Pick-Up sheet (Feb 27 BOB file)...")

xl_bob = pd.ExcelFile(FILE_BOB)
raw    = xl_bob.parse('Daily Pick-Up', header=None)

# Guard: verify correct hotel is selected
hotel_name = str(raw.iloc[3, 2]).strip()
if hotel_name != HOTEL_NAME:
    raise ValueError(
        f"\n\n  ❌ WRONG HOTEL IN BOB FILE\n"
        f"     Expected: '{HOTEL_NAME}'\n"
        f"     Found:    '{hotel_name}'\n\n"
        f"  Open the file, select Hickstead from the dropdown, save, re-run.\n"
    )
print(f"      Hotel confirmed: {hotel_name} ✅")

records = []
for i in range(9, len(raw)):
    row = raw.iloc[i]
    val = row.iloc[COL_DATE]
    if not isinstance(val, (pd.Timestamp, __import__('datetime').datetime)):
        continue

    def get(col):
        v = row.iloc[col] if col < len(row) else np.nan
        return float(v) if pd.notna(v) and str(v).strip() not in ['', 'nan'] else np.nan

    records.append({
        'date':      pd.Timestamp(val),
        'dow':       str(row.iloc[COL_DOW]).strip(),
        'month':     str(row.iloc[COL_MONTH]).strip(),
        'capacity':  get(COL_CAP),
        'ooo':       get(COL_OOO),
        'bob_sold':  get(COL_BOB_SOLD),
        'bob_occ':   get(COL_BOB_OCC),
        'bob_rev':   get(COL_BOB_REV),
        'bob_adr':   get(COL_BOB_ADR),
        'pickup_1d': get(COL_PICKUP_SOLD),
    })

df = pd.DataFrame(records)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').drop_duplicates('date').reset_index(drop=True)
print(f"      {len(df)} rows parsed | {df.date.min().date()} → {df.date.max().date()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — FILTER TO FUTURE DATES
# ─────────────────────────────────────────────────────────────────────────────
print("\n[2/7] Filtering to future dates...")
before = len(df)
df = df[df['date'] > EXTRACT_DATE].reset_index(drop=True)
print(f"      Kept {len(df)} rows (removed {before - len(df)} past/extract dates)")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — AVAILABLE ROOMS
# bob_occ in source = bob_sold / available_rooms (not /capacity).
# Verified: source occ matches sold/available exactly for all Jan rows.
# ─────────────────────────────────────────────────────────────────────────────
print("\n[3/7] Calculating available rooms...")
df['capacity']        = df['capacity'].fillna(HOTEL_CAP)
df['ooo']             = df['ooo'].fillna(0)
df['available_rooms'] = df['capacity'] - df['ooo']

# Recalculate bob_occ from sold/available — source confirmed to match this
df['bob_occ'] = (df['bob_sold'] / df['available_rooms']).round(4)
df['bob_occ'] = df['bob_occ'].clip(upper=1.0)

over = (df['bob_sold'] / df['available_rooms'] > 1.0).sum()
if over:
    print(f"      Capped {over} overbooking bob_occ values to 1.0")
print(f"      Avg capacity: {df['capacity'].mean():.0f} | avg OOO: {df['ooo'].mean():.1f} | avg available: {df['available_rooms'].mean():.1f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — LOAD 2025 ACTUALS FROM DEC 18 FILE (real STLY)
#
# The Feb 27 file's STLY and PACE columns are both zero for all future 2026
# dates — the PMS does not backfill last-year data for forward-looking BOB.
# The Dec 18 file's PickUp_datelink tab has Hickstead 2025 actuals (all 365
# days, snapshot taken Dec 18 2025 — effectively end-of-year final actuals).
# We join these onto 2026 dates using the exact date-minus-364-days offset
# (same DOW alignment as the day_by_day budget file).
# ─────────────────────────────────────────────────────────────────────────────
print("\n[4/7] Loading Hickstead 2025 actuals from Dec 18 file (real STLY)...")

xl_stly   = pd.ExcelFile(FILE_STLY)
raw_stly  = xl_stly.parse('PickUp_datelink', header=None, dtype=str)
headers   = raw_stly.iloc[0, :].tolist()
data_stly = raw_stly.iloc[1:].copy()
data_stly.columns = headers

hick_stly = data_stly[data_stly['Property Name'].str.contains('Hickstead', na=False)].copy()
hick_stly['date_2025']  = pd.to_datetime(hick_stly['Today.Occupancy Date'], errors='coerce')
hick_stly['stly_sold']  = pd.to_numeric(hick_stly['Today.Rooms Sold'], errors='coerce')
hick_stly['stly_rev']   = pd.to_numeric(hick_stly['Today.Room Revenue (EUR)'], errors='coerce')
hick_stly = hick_stly[hick_stly['date_2025'].notna()].copy()

print(f"      Hickstead 2025 rows: {len(hick_stly)} | {hick_stly.date_2025.min().date()} → {hick_stly.date_2025.max().date()}")

# Join by date - 364 days (same DOW offset used in day_by_day budget file)
# 2026-01-01 (Thu) → 2025-01-02 (Thu), 2026-01-02 (Fri) → 2025-01-03 (Fri) etc.
stly_lookup = hick_stly.set_index('date_2025')[['stly_sold', 'stly_rev']]

df['date_2025_equiv'] = df['date'] - pd.Timedelta(days=364)
df = df.merge(
    stly_lookup.rename_axis('date_2025_equiv').reset_index(),
    on='date_2025_equiv',
    how='left'
)

matched = df['stly_sold'].notna().sum()
print(f"      Matched {matched}/{len(df)} rows with 2025 actuals")
print(f"      Unmatched (Dec 2026 beyond Dec 2025 range): {len(df) - matched}")

# For the small number unmatched (late Dec 2026), fill with DOW median
dow_medians = df.dropna(subset=['stly_sold']).groupby('dow')[['stly_sold','stly_rev']].median()
for col in ['stly_sold', 'stly_rev']:
    mask = df[col].isna()
    if mask.any():
        df.loc[mask, col] = df.loc[mask, 'dow'].map(dow_medians[col])
        print(f"      Filled {mask.sum()} missing {col} with DOW median")

df = df.drop(columns=['date_2025_equiv'])


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — COMPUTE PICKUP FEATURES
# ─────────────────────────────────────────────────────────────────────────────
print("\n[5/7] Computing pickup features...")

# pickup_1d: negatives are valid (cancellations)
# Sort by date first to ensure rolling window is date-ordered
df = df.sort_values('date').reset_index(drop=True)
df['pickup_7d']       = df['pickup_1d'].rolling(window=7, min_periods=1).sum()
df['pickup_velocity'] = (df['pickup_7d'] / df['available_rooms']).round(4)

# pace_gap: how far ahead/behind 2025 actuals at same booking window
# Positive = more booked than last year at this point → strong demand signal
df['pace_gap'] = (df['bob_sold'] - df['stly_sold']).round(1)

print(f"      pickup_7d range     : {df['pickup_7d'].min():.0f} → {df['pickup_7d'].max():.0f} rooms")
print(f"      pickup_velocity range: {df['pickup_velocity'].min():.3f} → {df['pickup_velocity'].max():.3f}")
print(f"      pace_gap range      : {df['pace_gap'].min():.0f} → {df['pace_gap'].max():.0f} rooms")
print(f"      Avg pace_gap        : {df['pace_gap'].mean():+.1f} (+ = ahead of last year)")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — METADATA COLUMNS
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6/7] Adding metadata columns...")
df['extract_date'] = EXTRACT_DATE
df['days_ahead']   = (df['date'] - df['extract_date']).dt.days
df['date']         = df['date'].dt.strftime('%Y-%m-%d')
df['extract_date'] = df['extract_date'].dt.strftime('%Y-%m-%d')

# Final column order
df = df[[
    'date','dow','month','capacity','ooo','available_rooms',
    'bob_sold','bob_occ','bob_rev','bob_adr',
    'pickup_1d','pickup_7d','pickup_velocity',
    'stly_sold','stly_rev','pace_gap',
    'extract_date','days_ahead'
]]


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — SAVE
# ─────────────────────────────────────────────────────────────────────────────
print("\n[7/7] Saving...")
df.to_csv(OUTPUT, index=False)

print("\n" + "=" * 58)
print("✅  DONE")
print("=" * 58)
print(f"  Output:     {OUTPUT}")
print(f"  Total rows: {len(df)}")
print(f"  Columns:    {list(df.columns)}")

print()
print("  Monthly BOB snapshot (as of Feb 27 2026):")
print(f"  {'Month':<6} {'BOB Sold':>9} {'BOB Occ':>9} {'BOB ADR':>8} {'7d PU':>7} {'vs 2025':>8}")
print(f"  {'-'*52}")
df['date_dt'] = pd.to_datetime(df['date'])
for month in ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']:
    rows = df[df['month'] == month]
    if len(rows) == 0:
        continue
    sold = rows['bob_sold'].mean()
    occ  = rows['bob_occ'].mean()
    adr  = rows['bob_adr'].mean()
    pu   = rows['pickup_7d'].mean()
    gap  = rows['pace_gap'].mean()
    sign = '+' if gap >= 0 else ''
    print(f"  {month:<6} {sold:>9.1f} {occ:>9.1%} {adr:>8.0f} {pu:>7.1f} {sign}{gap:>7.1f}")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 58)
print("🔍  VALIDATION")
print("=" * 58)

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

check("Has 200+ future rows", len(df) >= 200, f"Only {len(df)}")
check("No duplicate dates", df['date'].duplicated().sum() == 0)
check("All dates after extract date",
      (pd.to_datetime(df['date']) > EXTRACT_DATE).all())
check("bob_occ between 0.0 and 1.0",
      ((df['bob_occ'] >= 0) & (df['bob_occ'] <= 1)).all())
check("bob_sold never exceeds capacity",
      (df['bob_sold'] <= df['capacity']).all())
check("available_rooms always > 0",
      (df['available_rooms'] > 0).all())
check("bob_adr realistic (£30–£400) where rooms sold",
      (df[df['bob_sold'] > 0]['bob_adr'].between(30, 400)).all(),
      f"Outliers: {df[(df['bob_sold']>0) & ~df['bob_adr'].between(30,400)]['bob_adr'].tolist()}")
check("pickup_velocity between −0.5 and 1.0",
      df['pickup_velocity'].between(-0.5, 1.0).all())
check("stly_sold populated (no zeros from missing data)",
      (df['stly_sold'] > 0).mean() > 0.5,
      f"Only {(df['stly_sold']>0).mean():.0%} non-zero")
check("pace_gap not all zero",
      df['pace_gap'].abs().sum() > 0,
      "All pace_gap = 0 means STLY was not loaded correctly")
# BOB naturally decreases with days_ahead (fewer rooms booked far in advance)
# Check the booking curve slopes correctly: near-term months have more BOB than far-out months
near_term = df[df['days_ahead'] <= 30]['bob_occ'].mean()
far_out   = df[df['days_ahead'] >= 90]['bob_occ'].mean()
check("Near-term BOB occ > far-out BOB occ (booking curve sanity)",
      near_term > far_out,
      f"Near ({near_term:.1%}) should be > far ({far_out:.1%})")
check("days_ahead always positive",
      (df['days_ahead'] > 0).all())

print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 All checks passed — clean_pickup.csv is ready")
else:
    print("  ⚠️  Fix the failed checks before running the next script")