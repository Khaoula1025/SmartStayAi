import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FILE_INPUT   = 'data/raw/Uno Hotels Pickup. 27.02.2026.xlsx'
OUTPUT       = 'data/processed/clean_pickup.csv'
HOTEL_CAP    = 52      # Hickstead total rooms
EXTRACT_DATE = '2026-02-27'

# Column positions in the Daily Pick-Up sheet (0-indexed)
COL_DATE         = 2
COL_DOW          = 3
COL_MONTH        = 4
COL_CAP          = 5
COL_OOO          = 6   # Out of order rooms
COL_BOB_SOLD     = 7   # Current BOB: rooms sold
COL_BOB_OCC      = 8   # Current BOB: occupancy rate
COL_BOB_REV      = 9   # Current BOB: revenue
COL_BOB_ADR      = 10  # Current BOB: ADR
COL_PICKUP_SOLD  = 20  # Pick-up: rooms sold delta (last 24hrs)
COL_STLY_SOLD    = 28  # STLY: same time last year rooms sold
COL_PACE_SOLD    = 35  # PACE: last year final actuals rooms sold
COL_PACE_OCC     = 36  # PACE: last year final actuals occ
COL_LY_REV       = 41  # LY actuals: revenue
COL_LY_RNS       = 44  # LY actuals: rooms night sold

print("=" * 55)
print("SmartStay — Script 04: Clean Pickup / BOB Data")
print("=" * 55)
print(f"  Extract date: {EXTRACT_DATE}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PARSE DAILY PICK-UP SHEET
# Header structure:
#   Row 5: Section labels (Current BOB, Prev BOB, Pick-up, STLY, PACE...)
#   Row 6: Column labels (Date, DOW, Cap, OOO, Sold, Occ, Rev, ADR...)
#   Row 7: Year label (2025)
#   Row 9+: Data rows, interspersed with "Weekly Total" summary rows
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/6] Parsing Daily Pick-Up sheet...")
xl  = pd.ExcelFile(FILE_INPUT)
raw = xl.parse('Daily Pick-Up', header=None)

records = []
for i in range(9, len(raw)):
    row = raw.iloc[i]
    val = row.iloc[COL_DATE]

    # Skip weekly total rows and empty rows
    if not isinstance(val, (pd.Timestamp, __import__('datetime').datetime)):
        continue

    def get(col):
        v = row.iloc[col] if col < len(row) else np.nan
        return float(v) if pd.notna(v) and str(v).strip() not in ['', 'nan'] else np.nan

    records.append({
        'date':         val,
        'dow':          str(row.iloc[COL_DOW]).strip(),
        'month':        str(row.iloc[COL_MONTH]).strip(),
        'capacity':     get(COL_CAP),
        'ooo':          get(COL_OOO),
        'bob_sold':     get(COL_BOB_SOLD),
        'bob_occ':      get(COL_BOB_OCC),
        'bob_rev':      get(COL_BOB_REV),
        'bob_adr':      get(COL_BOB_ADR),
        'pickup_1d':    get(COL_PICKUP_SOLD),
        'stly_sold':    get(COL_STLY_SOLD),
        'pace_sold':    get(COL_PACE_SOLD),
        'pace_occ':     get(COL_PACE_OCC),
        'ly_rev':       get(COL_LY_REV),
        'ly_rns':       get(COL_LY_RNS),
    })

df = pd.DataFrame(records)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').drop_duplicates('date').reset_index(drop=True)

print(f"      {len(df)} rows | {df.date.min().date()} → {df.date.max().date()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — FILTER TO FUTURE DATES ONLY
# The pickup file contains forward-looking BOB data.
# Keep only dates from the extract date onwards.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/6] Filtering to future dates (from extract date)...")
before = len(df)
df = df[df['date'] > EXTRACT_DATE].reset_index(drop=True)
print(f"      Kept {len(df)} future rows (removed {before - len(df)} past/current dates)")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — CALCULATE AVAILABLE ROOMS
# available = capacity - ooo (out of order rooms)
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/6] Calculating available rooms...")
df['capacity'] = df['capacity'].fillna(HOTEL_CAP)
df['ooo']      = df['ooo'].fillna(0)
df['available_rooms'] = df['capacity'] - df['ooo']
print(f"      avg capacity: {df['capacity'].mean():.0f}, avg OOO: {df['ooo'].mean():.1f}, avg available: {df['available_rooms'].mean():.1f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — COMPUTE PICKUP FEATURES
# pickup_7d     : rolling 7-day sum of daily pickup (rooms booked in last week)
# pickup_velocity: pickup_7d as a proportion of available capacity
#                  High velocity = strong demand signal → raise prices
# pace_gap      : how far ahead/behind last year's final actuals we are right now
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/6] Computing pickup features...")

# pickup_1d may have negatives (cancellations) — that's valid, keep them
df['pickup_7d']       = df['pickup_1d'].rolling(window=7, min_periods=1).sum()
df['pickup_velocity'] = (df['pickup_7d'] / df['available_rooms']).round(4)

# Pace gap: positive = ahead of LY, negative = behind
df['pace_gap'] = (df['bob_sold'] - df['pace_sold']).round(1)

# Recalculate bob_occ from raw numbers as cross-check
df['bob_occ_calc'] = (df['bob_sold'] / df['available_rooms']).round(4)

# If bob_occ is missing, use calculated value
df['bob_occ'] = df['bob_occ'].fillna(df['bob_occ_calc'])

print(f"      pickup_7d range: {df['pickup_7d'].min():.0f} → {df['pickup_7d'].max():.0f} rooms")
print(f"      pickup_velocity range: {df['pickup_velocity'].min():.3f} → {df['pickup_velocity'].max():.3f}")
print(f"      pace_gap range: {df['pace_gap'].min():.0f} → {df['pace_gap'].max():.0f} rooms")

# Drop the cross-check column
df = df.drop(columns=['bob_occ_calc'])


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — CLEAN UP AND ADD extract_date COLUMN
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/6] Final cleanup...")
df['extract_date'] = pd.to_datetime(EXTRACT_DATE)
df['days_ahead']   = (df['date'] - df['extract_date']).dt.days

# Clip bob_occ to max 1.0
over = (df['bob_occ'] > 1.0).sum()
df['bob_occ'] = df['bob_occ'].clip(upper=1.0)
if over:
    print(f"      Capped {over} bob_occ values above 1.0")

print(f"      days_ahead range: {df['days_ahead'].min()} → {df['days_ahead'].max()} days")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — SAVE
# ─────────────────────────────────────────────────────────────────────────────

print("\n[6/6] Saving...")
df.to_csv(OUTPUT, index=False)

print("\n" + "=" * 55)
print("✅  DONE")
print("=" * 55)
print(f"  Output:     {OUTPUT}")
print(f"  Total rows: {len(df)}")
print(f"  Columns:    {list(df.columns)}")
print()
print("  Monthly BOB snapshot (as of Feb 27 2026):")
print(f"  {'Month':<6} {'BOB Sold':>9} {'BOB Occ':>9} {'BOB ADR':>9} {'7d PU':>8} {'vs LY':>8}")
print(f"  {'-'*52}")
for month in ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']:
    rows = df[df['month'] == month]
    if len(rows) > 0:
        sold = rows['bob_sold'].mean()
        occ  = rows['bob_occ'].mean()
        adr  = rows['bob_adr'].mean()
        pu   = rows['pickup_7d'].mean()
        gap  = rows['pace_gap'].mean()
        sign = '+' if gap >= 0 else ''
        print(f"  {month:<6} {sold:>9.1f} {occ:>9.1%} {adr:>9.0f} {pu:>8.1f} {sign}{gap:>7.1f}")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 55)
print("🔍  VALIDATION")
print("=" * 55)

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

# 1. Has data
check("Has at least 200 future rows", len(df) >= 200, f"Only {len(df)} rows")

# 2. All dates are after extract date
past_dates = (df['date'] < EXTRACT_DATE).sum()
check("All dates are after extract date", past_dates == 0, f"{past_dates} past dates found")

# 3. No duplicate dates
dupes = df['date'].duplicated().sum()
check("No duplicate dates", dupes == 0, f"{dupes} duplicates")

# 4. bob_occ is 0–1
bad = ((df['bob_occ'] < 0) | (df['bob_occ'] > 1)).sum()
check("bob_occ between 0.0 and 1.0", bad == 0, f"{bad} out of range")

# 5. bob_sold never exceeds capacity
over_cap = (df['bob_sold'] > df['capacity']).sum()
check("bob_sold never exceeds capacity", over_cap == 0, f"{over_cap} rows oversold")

# 6. available_rooms is always positive
bad_avail = (df['available_rooms'] <= 0).sum()
check("available_rooms always > 0", bad_avail == 0, f"{bad_avail} rows with 0 or negative")

# 7. bob_adr is realistic when rooms are actually sold (£30–£400)
# Zero ADR is valid when bob_sold = 0 (no rooms booked yet for that date)
sold_rows = df[df['bob_sold'] > 0]
bad_adr = sold_rows['bob_adr'].notna() & ((sold_rows['bob_adr'] < 30) | (sold_rows['bob_adr'] > 400))
check(
    "bob_adr realistic (£30–£400) where rooms are sold",
    bad_adr.sum() == 0,
    f"{bad_adr.sum()} outliers where bob_sold > 0: {sold_rows.loc[bad_adr, 'bob_adr'].tolist()}"
)

# 8. pickup_velocity is reasonable (−0.5 to 1.0)
bad_vel = ((df['pickup_velocity'] < -0.5) | (df['pickup_velocity'] > 1.0)).sum()
check(
    "pickup_velocity between −0.5 and 1.0",
    bad_vel == 0,
    f"{bad_vel} unusual values"
)

# 9. Summer months have higher bob_sold than Jan (basic sanity)
jan = df[df['month'] == 'Jan']['bob_sold'].mean()
jul = df[df['month'] == 'Jul']['bob_sold'].mean()
if pd.notna(jan) and pd.notna(jul):
    check(
        f"July BOB ({jul:.1f}) higher than January BOB ({jan:.1f})",
        jul > jan,
        "Unexpected — summer should have more advance bookings"
    )

# 10. days_ahead is positive
bad_days = (df['days_ahead'] <= 0).sum()
check("days_ahead always positive", bad_days == 0, f"{bad_days} non-positive values")

print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 All checks passed — clean_pickup.csv is ready for the next step")
else:
    print("  ⚠️  Fix the failed checks before running the next script")