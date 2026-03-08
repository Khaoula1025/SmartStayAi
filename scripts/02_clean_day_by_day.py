import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FILE_INPUT = 'data/raw/Day by day budget split.xlsx'
OUTPUT     = 'data/processed/clean_day_by_day.csv'

print("=" * 55)
print("SmartStay — Script 02: Clean Day-by-Day Budget Split")
print("=" * 55)
# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PARSE
# Real header is at row 2 (rows 0 and 1 are merged title rows).
# Only keep the first 14 columns — cols 14–23 are completely empty.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/6] Parsing Hickstead sheet...")
xl = pd.ExcelFile(FILE_INPUT)
df = xl.parse('Hickstead', header=2)

# Keep only the 14 meaningful columns
df = df.iloc[:, :14].copy()

# Rename to clean snake_case names
df.columns = [
    'date_2026',   # 2026 date (the prediction date)
    'date_2025',   # DOW-matched 2025 reference date
    'dow',         # Day of week label (Mon, Tue...)
    'month',       # Month label (Jan, Feb...)
    'cs_occ',      # Competitor set occupancy 2025 actuals (decimal)
    'cs_adr',      # Competitor set ADR 2025 actuals (£)
    'h_occ',       # Hickstead occupancy 2025 actuals (decimal)
    'h_adr',       # Hickstead ADR 2025 actuals (£)
    'h_rns',       # Hickstead rooms night sold 2025 actuals
    'h_rev',       # Hickstead revenue 2025 actuals (£)
    'b_occ',       # Budget target OCC 2026 (decimal)
    'b_adr',       # Budget target ADR 2026 (£)
    'b_rns',       # Budget target RNS 2026
    'b_rev',       # Budget target Revenue 2026 (£)
]

print(f"      Parsed {len(df)} raw rows")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — FILTER VALID 2026 DATES ONLY
# The file has some summary/total rows that aren't real dates.
# Keep only rows where date_2026 is a real date in year 2026.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/6] Filtering to valid 2026 dates only...")
df['date_2026'] = pd.to_datetime(df['date_2026'], errors='coerce')
before = len(df)
df = df[df['date_2026'].dt.year == 2026].reset_index(drop=True)
print(f"      Removed {before - len(df)} non-date rows → {len(df)} rows remaining")
print(f"      Date range: {df.date_2026.min().date()} → {df.date_2026.max().date()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — HANDLE MISSING VALUES (December gap)
# December 2026 has 31 missing rows for h_adr, cs_occ, cs_adr, h_rns, h_rev.
# This is because the 2025 actuals for December weren't fully entered yet.
#
# Fix: fill each column using the monthly median for that month.
# e.g. if December h_adr is missing, fill with the median December h_adr
# from the rows that DO have values (earlier months with same month label).
#
# h_occ has NO missing values — it's already complete.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/6] Filling missing values...")

# Force all numeric columns to actual numeric type first
# (Excel sometimes imports them as objects/strings)
numeric_cols = ['cs_occ','cs_adr','h_occ','h_adr','h_rns','h_rev',
                'b_occ','b_adr','b_rns','b_rev']
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# ── Fix 1: December cs_occ, cs_adr, h_adr, h_rns, h_rev are ALL missing.
# Monthly median won't work when the entire month is empty.
# Solution: use the global median across all non-missing rows.

cols_with_gaps = ['cs_occ', 'cs_adr', 'h_adr', 'h_rns', 'h_rev']

for col in cols_with_gaps:
    missing_count = df[col].isna().sum()
    if missing_count == 0:
        print(f"      {col}: no missing values ✅")
        continue

    # Try monthly median first
    monthly_medians = df.groupby('month')[col].median()
    df[col] = df.apply(
        lambda row: monthly_medians.get(row['month'], float('nan'))
        if pd.isna(row[col]) else row[col],
        axis=1
    )

    # If still missing (whole month was empty), use global median
    still_missing = df[col].isna().sum()
    if still_missing > 0:
        global_median = df[col].median()
        df[col] = df[col].fillna(global_median)
        print(f"      {col}: filled {missing_count - still_missing} with monthly median, "
              f"{still_missing} with global median ({global_median:.2f})")
    else:
        print(f"      {col}: filled {missing_count} rows with monthly median")

# ── Fix 2: Jan 17 h_adr = £8.49 — clearly a data entry error.
# Replace with January median ADR.
jan_median_adr = df[df['month'] == 'Jan']['h_adr'].median()
bad_adr_mask = df['h_adr'] < 20
if bad_adr_mask.any():
    count = bad_adr_mask.sum()
    df.loc[bad_adr_mask, 'h_adr'] = jan_median_adr
    print(f"      h_adr: fixed {count} implausible value(s) < £20 → replaced with Jan median (£{jan_median_adr:.0f})")

# ── Fix 3: December b_occ and b_adr are 0 (budget not set, not genuinely zero).
# Treat as missing and fill with the global average budget values.
dec_mask = df['month'] == 'Dec'
for col in ['b_occ', 'b_adr', 'b_rns', 'b_rev']:
    df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
    zero_in_dec = (dec_mask & (df[col] == 0)).sum()
    if zero_in_dec > 0:
        global_avg = float(df.loc[~dec_mask, col].mean())
        df.loc[dec_mask & (df[col] == 0), col] = round(global_avg, 2)
        print(f"      {col}: replaced {zero_in_dec} December zeros with global avg ({global_avg:.2f})")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — VALIDATE OCC FORMAT
# h_occ and cs_occ should already be decimals (0–1).
# If any value > 1, it was stored as percentage — divide by 100.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/6] Validating occupancy format (should be 0–1 decimal)...")
for col in ['h_occ', 'cs_occ', 'b_occ']:
    if (df[col] > 1).any():
        count = (df[col] > 1).sum()
        df[col] = df[col] / 100
        print(f"      {col}: converted {count} values from % to decimal")
    else:
        print(f"      {col}: already decimal format ✅")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — ADD COMPUTED COLUMNS
# These are derived directly from existing columns.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/6] Adding computed columns...")

# Ensure all columns are numeric before computing
for col in ['h_occ','h_adr','cs_occ','cs_adr','b_occ','b_adr','b_rns','b_rev']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# RevPAR = OCC × ADR (revenue per available room)
df['h_revpar_2025']   = (df['h_occ'] * df['h_adr']).round(2)
df['cs_revpar_2025']  = (df['cs_occ'] * df['cs_adr']).round(2)

# Price position: how Hickstead ADR compares to comp set
df['price_vs_compset'] = (df['h_adr'] / df['cs_adr']).round(3)

# Budget gap: difference between budget target and 2025 actual
df['budget_occ_gap'] = (df['b_occ'] - df['h_occ']).round(4)
df['budget_adr_gap'] = (df['b_adr'] - df['h_adr']).round(2)

print("      Added: h_revpar_2025, cs_revpar_2025, price_vs_compset, budget_occ_gap, budget_adr_gap")


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
print("  Monthly avg 2025 actuals (h_occ vs cs_occ):")
print(f"  {'Month':<6} {'H OCC':>8} {'CS OCC':>8} {'H ADR':>8} {'CS ADR':>8} {'Price Pos':>10}")
print(f"  {'-'*50}")
for month in ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']:
    rows = df[df['month'] == month]
    if len(rows) > 0:
        h   = rows['h_occ'].mean()
        cs  = rows['cs_occ'].mean()
        adr = rows['h_adr'].mean()
        ca  = rows['cs_adr'].mean()
        pp  = rows['price_vs_compset'].mean()
        print(f"  {month:<6} {h:>7.0%} {cs:>8.0%} {adr:>7.0f} {ca:>8.0f} {pp:>10.2f}")


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

# 1. Exactly 365 rows
check(
    "Exactly 365 rows (one per day)",
    len(df) == 365,
    f"Got {len(df)} rows instead of 365"
)

# 2. Date range is full 2026
check(
    "Date range is 2026-01-01 → 2026-12-31",
    df['date_2026'].min().date() == pd.Timestamp('2026-01-01').date() and
    df['date_2026'].max().date() == pd.Timestamp('2026-12-31').date(),
    f"Got {df['date_2026'].min().date()} → {df['date_2026'].max().date()}"
)

# 3. No duplicate dates
dupes = df['date_2026'].duplicated().sum()
check(
    "No duplicate dates",
    dupes == 0,
    f"{dupes} duplicate dates found"
)

# 4. No missing values in critical columns
for col in ['h_occ', 'cs_occ', 'h_adr', 'cs_adr', 'b_occ', 'b_adr']:
    missing = df[col].isna().sum()
    check(
        f"No missing values in {col}",
        missing == 0,
        f"{missing} missing values remain"
    )

# 5. OCC values are all between 0 and 1
for col in ['h_occ', 'cs_occ', 'b_occ']:
    bad = ((df[col] < 0) | (df[col] > 1)).sum()
    check(
        f"{col} values between 0.0 and 1.0",
        bad == 0,
        f"{bad} values outside valid range"
    )

# 6. ADR values are realistic (£20–£500)
for col in ['h_adr', 'cs_adr', 'b_adr']:
    bad = ((df[col] < 20) | (df[col] > 500)).sum()
    check(
        f"{col} values realistic (£20–£500)",
        bad == 0,
        f"{bad} values outside £20–£500 range: {df[col][(df[col] < 20) | (df[col] > 500)].tolist()}"
    )

# 7. Seasonality — summer h_occ should be higher than winter
winter_occ = df[df['month'].isin(['Jan','Feb'])]['h_occ'].mean()
summer_occ = df[df['month'].isin(['Jun','Jul','Aug'])]['h_occ'].mean()
check(
    f"Seasonality correct: summer ({summer_occ:.1%}) > winter ({winter_occ:.1%})",
    summer_occ > winter_occ,
    "Summer occupancy is lower than winter — check data"
)

# 8. price_vs_compset is reasonable (0.5–2.0)
bad_pp = ((df['price_vs_compset'] < 0.5) | (df['price_vs_compset'] > 2.0)).sum()
check(
    "price_vs_compset between 0.5 and 2.0",
    bad_pp == 0,
    f"{bad_pp} rows have unusual price position"
)

print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 All checks passed — clean_day_by_day.csv is ready for the next step")
else:
    print("  ⚠️  Fix the failed checks before running the next script")