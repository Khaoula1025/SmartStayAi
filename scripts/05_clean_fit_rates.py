import pandas as pd
import numpy as np
import re

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FILE_INPUT       = 'data/raw/FIT Static Final Rates 2025-2026.xlsx'
OUTPUT_LOOKUP    = 'data/processed/clean_fit_rates.csv'
OUTPUT_BY_DATE   = 'data/processed/clean_floor_by_date.csv'

DAYS_OF_WEEK = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Hickstead room types (rows 44–47 low season, 52–55 high season)
HICKSTEAD_ROOMS = ['Double Room', 'Twin Room', 'Executive Double Room', 'Triple Room']

# The ML model uses "Double Room" as the reference room type (most common booking)
REFERENCE_ROOM = 'Double Room'

# Season date ranges
LOW_SEASON_RANGES = [
    ('2025-02-01', '2025-03-31'),
    ('2025-07-25', '2025-09-07'),
    ('2026-01-01', '2026-03-31'),
]
HIGH_SEASON_RANGES = [
    ('2025-04-01', '2025-07-24'),
    ('2025-09-08', '2025-12-31'),
]

print("=" * 55)
print("SmartStay — Script 05: Clean FIT Floor Rates")
print("=" * 55)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PARSE HICKSTEAD SECTION FROM FILE
# File contains rates for 4 hotels. We only need THE HICKSTEAD HOTEL section.
# Rows 41–58 in the raw sheet.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/4] Parsing Hickstead FIT rates...")
xl  = pd.ExcelFile(FILE_INPUT)
raw = xl.parse('Sheet1', header=None)

def parse_rate(val):
    """Convert '£85' or 85 to float 85.0"""
    if pd.isna(val):
        return np.nan
    s = str(val).replace('£', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return np.nan

def extract_hickstead_rates(raw, season_label, row_start):
    """
    Extract a block of rates for one season.
    Row structure:
      row_start+0: season description (e.g. 'FIT Static Rates - Low Season...')
      row_start+1: DOW headers (Mon, Tue, ...)
      row_start+2 to row_start+5: room type rows
    """
    records = []
    for offset in range(2, 6):  # 4 room type rows
        row_idx = row_start + offset
        if row_idx >= len(raw):
            break
        row = raw.iloc[row_idx]
        room_type = str(row.iloc[0]).strip()
        if room_type in ['nan', ''] or room_type not in HICKSTEAD_ROOMS:
            continue
        for d, day in enumerate(DAYS_OF_WEEK):
            rate = parse_rate(row.iloc[d + 1])  # cols 1–7 = Mon–Sun
            records.append({
                'hotel':      'Hickstead',
                'season':     season_label,
                'room_type':  room_type,
                'dow':        day,
                'floor_rate': rate,
            })
    return records

# Hickstead low season starts at row 42
# Hickstead high season starts at row 50
all_records = []
all_records += extract_hickstead_rates(raw, 'low',  row_start=42)
all_records += extract_hickstead_rates(raw, 'high', row_start=50)

df_rates = pd.DataFrame(all_records)
print(f"      Extracted {len(df_rates)} rate records")
print(f"      Room types: {df_rates['room_type'].unique().tolist()}")
print(f"      Seasons: {df_rates['season'].unique().tolist()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — SAVE LOOKUP TABLE
# Clean flat table: room_type × season × dow → floor_rate
# Useful for the pricing engine at query time
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/4] Saving lookup table...")
df_rates.to_csv(OUTPUT_LOOKUP, index=False)

print()
print("  Floor rates by room type and season:")
print(f"  {'Room Type':<30} {'Season':<8} {'Mon':>5} {'Tue':>5} {'Wed':>5} {'Thu':>5} {'Fri':>5} {'Sat':>5} {'Sun':>5}")
print(f"  {'-'*75}")
for room in HICKSTEAD_ROOMS:
    for season in ['low', 'high']:
        subset = df_rates[(df_rates['room_type'] == room) & (df_rates['season'] == season)]
        if len(subset) == 0:
            continue
        rates = []
        for day in DAYS_OF_WEEK:
            r = subset[subset['dow'] == day]['floor_rate']
            rates.append(f"{int(r.values[0]):>5}" if len(r) > 0 and pd.notna(r.values[0]) else '   --')
        print(f"  {room:<30} {season:<8} {''.join(rates)}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — BUILD DATE-LEVEL FLOOR PRICE TABLE
# For every date in 2025–2026, assign the correct season and DOW,
# then look up the floor price for the reference room (Double Room).
# This is what the pricing algorithm uses at runtime.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/4] Building date-level floor price table...")

# Generate every date in 2025–2026
dates = pd.date_range('2025-01-01', '2026-12-31')
df_dates = pd.DataFrame({'date': dates})
df_dates['dow']         = df_dates['date'].dt.day_name()
df_dates['is_weekend']  = df_dates['date'].dt.dayofweek.isin([4, 5, 6]).astype(int)
df_dates['year']        = df_dates['date'].dt.year
df_dates['month']       = df_dates['date'].dt.month

# Assign season
def get_season(date):
    for start, end in LOW_SEASON_RANGES:
        if pd.Timestamp(start) <= date <= pd.Timestamp(end):
            return 'low'
    for start, end in HIGH_SEASON_RANGES:
        if pd.Timestamp(start) <= date <= pd.Timestamp(end):
            return 'high'
    return None  # dates not covered by any range (e.g. Jan 2025)

df_dates['season'] = df_dates['date'].apply(get_season)

# Build a quick lookup dict: (season, dow) → floor_rate for reference room
ref_rates = df_rates[df_rates['room_type'] == REFERENCE_ROOM]
rate_lookup = {
    (row['season'], row['dow']): row['floor_rate']
    for _, row in ref_rates.iterrows()
}

df_dates['floor_price'] = df_dates.apply(
    lambda r: rate_lookup.get((r['season'], r['dow']), np.nan),
    axis=1
)

# Report coverage
covered    = df_dates['floor_price'].notna().sum()
uncovered  = df_dates['floor_price'].isna().sum()
print(f"      {covered} dates with floor price assigned")
print(f"      {uncovered} dates without season assignment (outside contract periods)")
print(f"      Reference room: {REFERENCE_ROOM}")

# Show the uncovered dates
if uncovered > 0:
    unc = df_dates[df_dates['floor_price'].isna()]
    print(f"      Uncovered period: {unc['date'].min().date()} → {unc['date'].max().date()}")
    # Fill uncovered dates with low season rate (safest default)
    df_dates['floor_price'] = df_dates.apply(
        lambda r: rate_lookup.get(('low', r['dow']), np.nan)
        if pd.isna(r['floor_price']) else r['floor_price'],
        axis=1
    )
    df_dates['season'] = df_dates['season'].fillna('low')  # label filled dates as low season
    print(f"      Filled with low season rate as conservative default")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — SAVE AND VALIDATE
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/4] Saving and validating...")
df_dates.to_csv(OUTPUT_BY_DATE, index=False)

print("\n" + "=" * 55)
print("✅  DONE")
print("=" * 55)
print(f"  Output 1: {OUTPUT_LOOKUP}   ({len(df_rates)} rows — rate lookup table)")
print(f"  Output 2: {OUTPUT_BY_DATE}  ({len(df_dates)} rows — one floor price per date)")
print()
print("  Floor price summary by season:")
for season in ['low', 'high']:
    rows = df_dates[df_dates['season'] == season]
    print(f"    {season.capitalize()} season: £{rows['floor_price'].min():.0f}–£{rows['floor_price'].max():.0f} "
          f"(avg £{rows['floor_price'].mean():.0f}, {len(rows)} days)")


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

# Lookup table checks
check("Lookup table has 56 rows (4 rooms × 2 seasons × 7 days)",
      len(df_rates) == 56, f"Got {len(df_rates)}")

check("All 4 Hickstead room types present",
      set(df_rates['room_type'].unique()) == set(HICKSTEAD_ROOMS),
      f"Found: {df_rates['room_type'].unique().tolist()}")

check("No missing floor rates in lookup",
      df_rates['floor_rate'].notna().all(),
      f"{df_rates['floor_rate'].isna().sum()} missing")

check("Floor rates realistic (£50–£200)",
      ((df_rates['floor_rate'] >= 50) & (df_rates['floor_rate'] <= 200)).all(),
      f"Out of range: {df_rates[~df_rates['floor_rate'].between(50,200)]['floor_rate'].tolist()}")

check("High season rates > low season rates for same room/DOW",
      df_rates.groupby(['room_type','dow']).apply(
          lambda g: g[g['season']=='high']['floor_rate'].values[0] >
                    g[g['season']=='low']['floor_rate'].values[0]
          if len(g) == 2 else True
      ).all(),
      "Some high season rates are not above low season")

# Date table checks
check("Date table has 730 rows (2025 + 2026)",
      len(df_dates) == 730, f"Got {len(df_dates)}")

check("No missing floor prices after filling",
      df_dates['floor_price'].notna().all(),
      f"{df_dates['floor_price'].isna().sum()} still missing")

check("Season assigned to all dates",
      df_dates['season'].notna().all(),
      f"{df_dates['season'].isna().sum()} dates without season")

print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 All checks passed — FIT rate files are ready for the final merge")
else:
    print("  ⚠️  Fix the failed checks before running the merge script")