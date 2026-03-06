"""
SmartStay Intelligence — Hickstead Hotel
Script 01: Clean Occupancy Data (2024 + 2025)

Input files:
  - UNOHICK_Occupancy_20260227_235408.xlsx  (2024 full year)
  - UNOHICK_Occupancy_20260227_235324.xlsx  (2025 full year)

Output:
  - clean_occupancy.csv  (433+ rows, one per day)

Run:
  python 01_clean_occupancy.py
"""

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — update paths if needed
# ─────────────────────────────────────────────────────────────────────────────
FILE_2024 = 'data/raw/UNOHICK_Occupancy_20260227_235408.xlsx'
FILE_2025 = 'data/raw/UNOHICK_Occupancy_20260227_235324.xlsx'
OUTPUT    = 'data/processed//clean_hicksteads_occupancy.csv'
TOTAL_ROOMS = 52  # Hickstead confirmed capacity


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PARSER
# Extracts one row per day from the block-based PMS export format.
#
# The raw file structure per block (repeats every ~8 rows):
#   Row A: "Room Type" | date1 | ... | date2 | ... | date3 | ... | date4 | ... | date5
#   Row B: column labels (Avl, Ov, Let, Tot, Occ%, Slprs)
#   Row C: DB room type data
#   Row D: DB_SB room type data
#   Row E: EXEC room type data
#   Row F: TB room type data
#   Row G: "Totals" | aggregated values  ← THIS IS THE ONLY ROW WE NEED
#
# Column offsets in the Totals row:
#   Date 1 (base col 1):  Let=+2, Tot=+4, Occ%=+6, Slprs=+8  (wider, has NaN gaps)
#   Date 2-5 (base col N): Let=+2, Tot=+3, Occ%=+4, Slprs=+5  (compact, no gaps)
# ─────────────────────────────────────────────────────────────────────────────

def parse_occupancy_file(filepath):
    """Parse a single PMS occupancy xlsx file into a clean daily dataframe."""

    xl  = pd.ExcelFile(filepath)
    raw = xl.parse('Sheet1', header=None, dtype=str)

    # Pre-clean: remove page-break rows ("Page X of Y at...")
    raw = raw[~raw[0].astype(str).str.startswith('Page')].reset_index(drop=True)
    # Also remove hotel title rows that appear after page breaks
    raw = raw[~raw[0].astype(str).str.startswith('The Hickstead')].reset_index(drop=True)

    records = []

    for i in range(len(raw)):
        row = raw.iloc[i]

        # Only process "Room Type" header rows that have at least one date
        if str(row[0]) != 'Room Type':
            continue

        # ── Collect the (up to 5) dates and their base column positions
        date_positions = []
        for c in range(1, len(row)):
            val = str(row[c]) if pd.notna(row[c]) else ''
            if any(m in val for m in ['Jan','Feb','Mar','Apr','May','Jun',
                                       'Jul','Aug','Sep','Oct','Nov','Dec']):
                date_positions.append((c, val.strip()))

        # Skip Room Type rows with no dates (page-break artifacts)
        if not date_positions:
            continue

        # ── Find the Totals row within the next 15 rows
        totals_row = None
        for j in range(i + 1, min(i + 15, len(raw))):
            if str(raw.iloc[j, 0]) == 'Totals':
                totals_row = raw.iloc[j]
                break

        if totals_row is None:
            continue  # skip blocks with no Totals row

        # ── Extract values for each date in the block
        for k, (base, date_str) in enumerate(date_positions):
            try:
                date = pd.to_datetime(date_str, format='%a %d %b %Y')

                if k == 0:
                    # First date has NaN spacer columns between values
                    let_  = totals_row.iloc[base + 2]   # rooms let
                    tot   = totals_row.iloc[base + 4]   # total rooms
                    occ   = totals_row.iloc[base + 6]   # occupancy %
                    slprs = totals_row.iloc[base + 8]   # sleepers
                else:
                    # Remaining dates are compact
                    let_  = totals_row.iloc[base + 2]
                    tot   = totals_row.iloc[base + 3]
                    occ   = totals_row.iloc[base + 4]
                    slprs = totals_row.iloc[base + 5]

                def to_float(v):
                    return float(v) if str(v) not in ['nan', ''] else None

                records.append({
                    'date':        date,
                    'rooms_let':   to_float(let_),
                    'total_rooms': to_float(tot),
                    'occ_pct':     to_float(occ),
                    'sleepers':    to_float(slprs),
                })

            except Exception:
                pass  # skip malformed entries silently

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])

    # Drop the annual summary row if it sneaked in (year-level Totals)
    df = df[df['date'].dt.year.isin([2024, 2025, 2026])]
    df = df.sort_values('date').drop_duplicates('date').reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — PARSE BOTH FILES
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 55)
print("SmartStay — Script 01: Clean Occupancy Data")
print("=" * 55)

print("\n[1/7] Parsing 2024 file...")
df_2024 = parse_occupancy_file(FILE_2024)
print(f"      {len(df_2024)} rows | {df_2024.date.min().date()} → {df_2024.date.max().date()}")

print("\n[2/7] Parsing 2025 file...")
df_2025 = parse_occupancy_file(FILE_2025)
print(f"      {len(df_2025)} rows | {df_2025.date.min().date()} → {df_2025.date.max().date()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — REMOVE CLOSED PERIOD (Jan/Feb/Mar 2024)
# The hotel was not operational or the PMS was not recording.
# All values are 0 — keeping them would teach the model wrong patterns.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/7] Removing Jan/Feb/Mar 2024 (hotel closed — all zeros)...")
before = len(df_2024)
df_2024 = df_2024[
    ~((df_2024['date'].dt.year == 2024) & (df_2024['date'].dt.month <= 3))
].reset_index(drop=True)
print(f"      Removed {before - len(df_2024)} rows → {len(df_2024)} remaining")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — COMBINE 2024 + 2025
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/7] Combining 2024 + 2025...")
df = pd.concat([df_2024, df_2025]).sort_values('date').drop_duplicates('date').reset_index(drop=True)
print(f"      {len(df)} rows | {df.date.min().date()} → {df.date.max().date()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — CONVERT OCC_PCT (0–100) TO OCC_RATE (0–1) AND CAP AT 100%
# The PMS exports occupancy as a percentage (e.g. 74).
# The ML model works with decimals (0.74).
# One value of 102% exists — this is a data entry error, capped to 1.0.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/7] Converting occ_pct → occ_rate and capping at 100%...")
df['occ_rate'] = pd.to_numeric(df['occ_pct'], errors='coerce') / 100
over_100 = (df['occ_rate'] > 1.0).sum()
df['occ_rate'] = df['occ_rate'].clip(upper=1.0)
print(f"      {over_100} rows were above 100% → capped to 1.0")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — ADD CALENDAR FEATURES
# These are computed directly from the date — no external data needed.
# They will be used as features in the ML model.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[6/7] Adding calendar features...")
df['year']           = df['date'].dt.year
df['month']          = df['date'].dt.month
df['dow']            = df['date'].dt.day_name().str[:3]   # Mon, Tue, Wed...
df['day_of_week']    = df['date'].dt.dayofweek             # 0=Mon, 6=Sun
df['week_of_year']   = df['date'].dt.isocalendar().week.astype(int)
df['is_weekend']     = df['date'].dt.dayofweek.isin([4, 5, 6]).astype(int)
df['is_high_season'] = df['month'].isin([4,5,6,7,9,10,11,12]).astype(int)
print("      Added: year, month, dow, day_of_week, week_of_year, is_weekend, is_high_season")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — FILL SMALL GAPS (forward fill, max 3 days)
# Some dates have NaN occupancy due to page breaks in the PMS export.
# We fill gaps of up to 3 consecutive days using the previous day's value.
# Gaps longer than 3 days are left as NaN — they'll be filled during merge.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[7/7] Filling small gaps (forward fill, max 3 consecutive days)...")
missing_before = df['occ_rate'].isna().sum()
df['occ_rate'] = df['occ_rate'].ffill(limit=3)
missing_after  = df['occ_rate'].isna().sum()
print(f"      Filled {missing_before - missing_after} rows")
print(f"      Still missing: {missing_after} rows (will merge from day_by_day later)")


# ─────────────────────────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────────────────────────

df.to_csv(OUTPUT, index=False)

print("\n" + "=" * 55)
print("✅  DONE")
print("=" * 55)
print(f"  Output:     {OUTPUT}")
print(f"  Total rows: {len(df)}")
print(f"  Columns:    {list(df.columns)}")
print()
print("  Monthly avg occupancy:")
monthly = df.groupby(df['date'].dt.to_period('M'))['occ_rate'].mean()
for period, val in monthly.items():
    if pd.notna(val):
        bar = '█' * int(val * 25)
        print(f"    {period}   {bar:<25} {val:.1%}")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION — runs automatically after saving
# Each check prints PASS or FAIL with a clear explanation.
# If anything FAILs, fix the issue before moving to the next script.
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

# 1. Row count — should have at least 400 rows (20 months × ~20 days minimum)
check(
    "Row count ≥ 400",
    len(df) >= 400,
    f"Only {len(df)} rows — check that both files parsed correctly"
)

# 2. Date range — should start in 2024 and end in 2025
check(
    "Date range starts in 2024",
    df['date'].dt.year.min() == 2024,
    f"Earliest year found: {df['date'].dt.year.min()}"
)
check(
    "Date range ends in 2025",
    df['date'].dt.year.max() == 2025,
    f"Latest year found: {df['date'].dt.year.max()}"
)

# 3. No duplicate dates
dupes = df['date'].duplicated().sum()
check(
    "No duplicate dates",
    dupes == 0,
    f"{dupes} duplicate dates found — check drop_duplicates step"
)

# 4. Dates are sorted ascending
check(
    "Dates sorted in order",
    df['date'].is_monotonic_increasing,
    "Dates are not in order — check sort_values step"
)

# 5. occ_rate is between 0 and 1
bad_occ = ((df['occ_rate'] < 0) | (df['occ_rate'] > 1)).sum()
check(
    "occ_rate all between 0.0 and 1.0",
    bad_occ == 0,
    f"{bad_occ} rows have occ_rate outside 0–1 range"
)

# 6. No Jan/Feb/Mar 2024 rows (hotel was closed)
closed_rows = df[(df['date'].dt.year == 2024) & (df['date'].dt.month <= 3)]
check(
    "No Jan/Feb/Mar 2024 rows (closed period removed)",
    len(closed_rows) == 0,
    f"{len(closed_rows)} rows from closed period still present"
)

# 7. Summer 2025 occupancy looks realistic (should be above 70%)
summer_2025 = df[(df['date'].dt.year == 2025) & (df['date'].dt.month.isin([6, 7, 8]))]
summer_avg  = summer_2025['occ_rate'].mean()
check(
    f"Summer 2025 avg occupancy looks realistic (>70%) — got {summer_avg:.1%}",
    summer_avg > 0.70,
    f"Summer avg is {summer_avg:.1%} — unusually low, check parsing"
)

# 8. Jan 2025 occupancy is lower than July 2025 (basic seasonality check)
jan_avg = df[(df['date'].dt.year == 2025) & (df['date'].dt.month == 1)]['occ_rate'].mean()
jul_avg = df[(df['date'].dt.year == 2025) & (df['date'].dt.month == 7)]['occ_rate'].mean()
check(
    f"Seasonality correct: Jan 2025 ({jan_avg:.1%}) < Jul 2025 ({jul_avg:.1%})",
    jan_avg < jul_avg,
    "Jan is higher than Jul — something is wrong with the date parsing"
)

# 9. total_rooms should never exceed 52 (Hickstead capacity)
# Values below 52 are normal — some rooms may be Out of Order on any given night
wrong_capacity = df[df['total_rooms'].notna() & (df['total_rooms'] > 52)]
check(
    "Total rooms never exceeds 52 (Hickstead capacity)",
    len(wrong_capacity) == 0,
    f"{len(wrong_capacity)} rows have total_rooms > 52: {wrong_capacity['total_rooms'].unique()}"
)

# 10. Calendar columns exist and have correct value ranges
check(
    "day_of_week values are 0–6",
    df['day_of_week'].between(0, 6).all(),
    f"Found values: {df['day_of_week'].unique()}"
)
check(
    "month values are 1–12",
    df['month'].between(1, 12).all(),
    f"Found values: {df['month'].unique()}"
)
check(
    "is_weekend is only 0 or 1",
    df['is_weekend'].isin([0, 1]).all(),
    f"Found values: {df['is_weekend'].unique()}"
)
check(
    "is_high_season is only 0 or 1",
    df['is_high_season'].isin([0, 1]).all(),
    f"Found values: {df['is_high_season'].unique()}"
)

# 11. Missing value report
missing_occ = df['occ_rate'].isna().sum()
missing_pct = missing_occ / len(df) * 100
check(
    f"Missing occ_rate under 40% of rows — got {missing_pct:.1f}% missing",
    missing_pct < 40,
    f"{missing_occ} rows have no occupancy — more than expected, check source files"
)

# ── SUMMARY
print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 All checks passed — clean_occupancy.csv is ready for the next step")
else:
    print("  ⚠️  Fix the failed checks before running the next script")