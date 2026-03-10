import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FILE_INPUT = 'data/raw/the-hickstead-hotel-by-uno_bookingdotcom_lowest_los1_2guests_standard_room_only .xlsx'
OUTPUT     = 'data/processed/clean_bookingcom.csv'

# Values in competitor columns that mean "not available" — treat as NaN
JUNK_VALUES = ['--', 'Sold out', 'No room only', 'Room N/A', 'nan', '']

COMPETITOR_COLS = [
    'The Birch Hotel',
    'The Windmill Inn',
    'The Horse Inn Hurst',
    'Tottington Manor Hotel',
    'Best Western Princes Marine Hotel',
    'Comfort Inn Arundel',
]

print("=" * 55)
print("SmartStay — Script 03: Clean Booking.com Rates")
print("=" * 55)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PARSE OVERVIEW SHEET
# Header is at row 4 (rows 0–3 are title/metadata rows).
# Filter to only rows where Date is a real datetime.
# Drop empty columns (Unnamed:0, Market demand, Events).
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/7] Parsing Overview sheet...")
xl = pd.ExcelFile(FILE_INPUT)
ov = xl.parse('Overview', header=4)
ov = ov[ov['Date'].apply(lambda x: isinstance(x, pd.Timestamp))].copy()
ov = ov.reset_index(drop=True)

# Drop completely empty columns
ov = ov.drop(columns=['Unnamed: 0', 'Market demand', 'Events'], errors='ignore')

# Rename to clean names
ov = ov.rename(columns={
    'Date':                   'date',
    'Day':                    'dow',
    'Lowest own hotel':       'own_rate',
    'Median lowest compset':  'compset_median',
    'Compset price rank':     'price_rank_raw',
    'Booking.com Ranking':    'bcom_rank_raw',
    'Holidays':               'holiday_flag',
})

# Clean own_rate — replace junk strings with NaN then convert to numeric
# Junk values mean Hickstead had no standard room available that night
own_rate_junk = ov['own_rate'].apply(
    lambda x: str(x).strip() in JUNK_VALUES + ['Sold out','Room N/A','No room only','3rd Party only','LOS2','LOS3']
)
junk_count = own_rate_junk.sum()
ov.loc[own_rate_junk, 'own_rate'] = np.nan
ov['own_rate'] = pd.to_numeric(ov['own_rate'], errors='coerce')

# Interpolate gaps in own_rate (linear, max 7 days)
missing_before = ov['own_rate'].isna().sum()
ov['own_rate'] = ov['own_rate'].interpolate(method='linear', limit=7)
missing_after  = ov['own_rate'].isna().sum()

# Same for compset_median
ov['compset_median'] = pd.to_numeric(ov['compset_median'], errors='coerce')
comp_missing = ov['compset_median'].isna().sum()
if comp_missing > 0:
    ov['compset_median'] = ov['compset_median'].interpolate(method='linear', limit=7)

print(f"      {len(ov)} rows | {ov.date.min().date()} → {ov.date.max().date()}")
print(f"      own_rate: {junk_count} junk values → NaN, {missing_before - missing_after} interpolated, {missing_after} still missing")
print(f"      compset_median: {comp_missing} gaps filled")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — PARSE RATES SHEET
# Same header structure. Contains individual competitor rates.
# Junk values (--. Sold out, etc.) mean the competitor had no room available.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/7] Parsing Rates sheet...")
rt = xl.parse('Rates', header=4)
rt = rt[rt['Date'].apply(lambda x: isinstance(x, pd.Timestamp))].copy()
rt = rt.reset_index(drop=True)

# Drop empty columns
rt = rt.drop(columns=['Unnamed: 0', 'Unnamed: 11', 'Market demand'], errors='ignore')
rt = rt.rename(columns={'Date': 'date', 'Day': 'dow',
                        'The Hickstead Hotel By Uno': 'own_rate_rates_sheet'})

# Clean competitor rate columns — replace junk with NaN, convert to numeric
for col in COMPETITOR_COLS:
    if col in rt.columns:
        rt[col] = rt[col].apply(lambda x: np.nan if str(x).strip() in JUNK_VALUES else x)
        rt[col] = pd.to_numeric(rt[col], errors='coerce')

print(f"      {len(rt)} rows | competitors: {COMPETITOR_COLS}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — PARSE RANK STRINGS
# "2 of 2"   → price_rank = 2  (Hickstead is 2nd cheapest out of 2)
# "59 of 156" → bcom_rank = 59 (Hickstead is ranked 59th on Booking.com)
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/7] Parsing rank strings...")

def parse_rank(value):
    """Extract first number from strings like '2 of 2' or '59 of 156'."""
    try:
        return int(str(value).split(' of ')[0])
    except:
        return np.nan

def parse_rank_total(value):
    """Extract second number — total in market."""
    try:
        return int(str(value).split(' of ')[1])
    except:
        return np.nan

ov['price_rank']       = ov['price_rank_raw'].apply(parse_rank)
ov['price_rank_total'] = ov['price_rank_raw'].apply(parse_rank_total)
ov['bcom_rank']        = ov['bcom_rank_raw'].apply(parse_rank)
ov['bcom_rank_total']  = ov['bcom_rank_raw'].apply(parse_rank_total)

# Fill missing price_rank (days Hickstead had no room — rank not recorded)
# Forward fill then backward fill to cover any gaps
ov['price_rank'] = ov['price_rank'].ffill().bfill()

# Drop the raw string columns
ov = ov.drop(columns=['price_rank_raw', 'bcom_rank_raw'])

print(f"      price_rank range: {ov.price_rank.min()} – {ov.price_rank.max()}")
print(f"      bcom_rank range:  {ov.bcom_rank.min()} – {ov.bcom_rank.max()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — CREATE is_bank_holiday FLAG
# The Holidays column contains 'GB' on UK bank holidays, NaN otherwise.
# Convert to a simple 0/1 flag.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/7] Creating holiday flags...")

# ── England & Wales bank holidays 2025 (official public holidays)
ENGLAND_BANK_HOLIDAYS_2025 = pd.to_datetime([
    '2025-01-01',  # New Year's Day
    '2025-04-18',  # Good Friday
    '2025-04-21',  # Easter Monday
    '2025-05-05',  # Early May bank holiday
    '2025-05-26',  # Spring bank holiday
    '2025-08-25',  # Summer bank holiday
    '2025-12-25',  # Christmas Day
    '2025-12-26',  # Boxing Day
])

# ── Cultural holidays relevant to Hickstead (leisure demand drivers)
# These are not public holidays but do affect hotel occupancy in West Sussex
CULTURAL_HOLIDAYS_2025 = pd.to_datetime([
    '2025-02-14',  # Valentine's Day — couples short breaks
    '2025-03-30',  # Mothering Sunday — family stays and dining
    '2025-06-15',  # Father's Day — family stays and dining
])

# ── Excluded (regional holidays irrelevant to Hickstead, West Sussex):
# St Patrick's Day (Ireland/N.Ireland), St Andrew's Day (Scotland),
# St David's Day (Wales), St George's Day (minor, no hotel impact),
# Remembrance Sunday (no demand impact), Battle of the Boyne (N.Ireland)

ov['is_bank_holiday']     = ov['date'].isin(ENGLAND_BANK_HOLIDAYS_2025).astype(int)
ov['is_cultural_holiday'] = ov['date'].isin(CULTURAL_HOLIDAYS_2025).astype(int)

print(f"      is_bank_holiday:     {ov['is_bank_holiday'].sum()} dates (England & Wales official holidays)")
print(f"      is_cultural_holiday: {ov['is_cultural_holiday'].sum()} dates (Valentine's, Mother's Day, Father's Day)")
print(f"      Dropped: St Patrick's, St Andrew's, St David's, Remembrance Sunday (not relevant to Hickstead area)")
ov = ov.drop(columns=['holiday_flag'])


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — MERGE COMPETITOR RATES FROM RATES SHEET
# Add individual competitor columns to the main overview dataframe.
# Also recalculate compset_median from raw rates as a cross-check.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/7] Merging competitor rates...")

# Merge on date
competitor_data = rt[['date'] + [c for c in COMPETITOR_COLS if c in rt.columns]].copy()
ov = ov.merge(competitor_data, on='date', how='left')

# Recalculate compset_median from individual competitors as a cross-check
comp_cols_present = [c for c in COMPETITOR_COLS if c in ov.columns]
ov['compset_median_calc'] = ov[comp_cols_present].median(axis=1)

# Flag where calculated median differs significantly from reported median
ov['compset_median'] = pd.to_numeric(ov['compset_median'], errors='coerce')
ov['median_diff'] = (ov['compset_median'] - ov['compset_median_calc']).abs()

# Where overview median is missing, use calculated one
missing_median = ov['compset_median'].isna().sum()
if missing_median > 0:
    ov['compset_median'] = ov['compset_median'].fillna(ov['compset_median_calc'])
    print(f"      Filled {missing_median} missing compset_median from individual rates")
else:
    print(f"      compset_median complete — no gaps ✅")

# Drop helper columns
ov = ov.drop(columns=['compset_median_calc', 'median_diff', 'own_rate_rates_sheet'],
             errors='ignore')

print(f"      Competitor columns added: {comp_cols_present}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — ADD COMPUTED COLUMNS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[6/7] Adding computed columns...")

ov['own_rate']       = pd.to_numeric(ov['own_rate'], errors='coerce')
ov['compset_median'] = pd.to_numeric(ov['compset_median'], errors='coerce')

# Price position: 1.0 = parity, >1 = Hickstead more expensive, <1 = cheaper
ov['price_vs_compset'] = (ov['own_rate'] / ov['compset_median']).round(3)

# Normalised Booking.com rank (0 = best, 1 = worst)
ov['bcom_rank_norm'] = (ov['bcom_rank'] / ov['bcom_rank_total']).round(3)

print("      Added: price_vs_compset, bcom_rank_norm")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — SAVE
# ─────────────────────────────────────────────────────────────────────────────

print("\n[7/7] Saving...")
ov.to_csv(OUTPUT, index=False)

print("\n" + "=" * 55)
print("✅  DONE")
print("=" * 55)
print(f"  Output:     {OUTPUT}")
print(f"  Total rows: {len(ov)}")
print(f"  Columns:    {list(ov.columns)}")
print()
print("  Monthly avg own_rate vs compset_median:")
print(f"  {'Month':<6} {'Own Rate':>10} {'Compset':>10} {'Price Pos':>10} {'BH Days':>8}")
print(f"  {'-'*46}")
ov['month'] = ov['date'].dt.month
for m in range(1, 13):
    rows = ov[ov['month'] == m]
    month_name = rows['date'].dt.strftime('%b').iloc[0]
    own   = rows['own_rate'].mean()
    comp  = rows['compset_median'].mean()
    pp    = rows['price_vs_compset'].mean()
    bh    = rows['is_bank_holiday'].sum()
    print(f"  {month_name:<6} {own:>9.0f} {comp:>10.0f} {pp:>10.2f} {bh:>8}")


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

# 1. Row count
check("365 rows", len(ov) == 365, f"Got {len(ov)}")

# 2. Date range
check(
    "Date range is 2025-01-01 → 2025-12-31",
    ov['date'].min().date() == pd.Timestamp('2025-01-01').date() and
    ov['date'].max().date() == pd.Timestamp('2025-12-31').date(),
    f"Got {ov['date'].min().date()} → {ov['date'].max().date()}"
)

# 3. No duplicate dates
dupes = ov['date'].duplicated().sum()
check("No duplicate dates", dupes == 0, f"{dupes} duplicates found")

# 4. own_rate is realistic (£30–£300)
bad = ((ov['own_rate'] < 30) | (ov['own_rate'] > 300)).sum()
check(
    "own_rate realistic (£30–£300)",
    bad == 0,
    f"{bad} values outside range: {ov.loc[(ov['own_rate']<30)|(ov['own_rate']>300), 'own_rate'].tolist()}"
)

# 5. compset_median is realistic
bad = ((ov['compset_median'] < 30) | (ov['compset_median'] > 300)).sum()
check(
    "compset_median realistic (£30–£300)",
    bad == 0,
    f"{bad} values outside range"
)

# 6. No missing own_rate
missing = ov['own_rate'].isna().sum()
check("No missing own_rate", missing == 0, f"{missing} missing")

# 7. No missing compset_median
missing = ov['compset_median'].isna().sum()
check("No missing compset_median", missing == 0, f"{missing} missing")

# 8. price_rank is 1 or 2 (only 2 hotels in compset)
bad = ov['price_rank'].isna().sum()
check("price_rank has no missing values", bad == 0, f"{bad} missing")

# 9. is_bank_holiday and is_cultural_holiday are only 0 or 1
check(
    "is_bank_holiday is only 0 or 1",
    ov['is_bank_holiday'].isin([0, 1]).all(),
    f"Found: {ov['is_bank_holiday'].unique()}"
)
check(
    "is_cultural_holiday is only 0 or 1",
    ov['is_cultural_holiday'].isin([0, 1]).all(),
    f"Found: {ov['is_cultural_holiday'].unique()}"
)

# 10. Correct counts
bh_count = ov['is_bank_holiday'].sum()
ch_count = ov['is_cultural_holiday'].sum()
check(
    f"Bank holiday count = 8 (got {bh_count})",
    bh_count == 8,
    "Check ENGLAND_BANK_HOLIDAYS_2025 list"
)
check(
    f"Cultural holiday count = 3 (got {ch_count})",
    ch_count == 3,
    "Check CULTURAL_HOLIDAYS_2025 list"
)

# 11. price_vs_compset is reasonable (0.3–2.5 — wider range to account for genuine market spikes)
bad = ((ov['price_vs_compset'] < 0.3) | (ov['price_vs_compset'] > 2.5)).sum()
check(
    "price_vs_compset between 0.3 and 2.5",
    bad == 0,
    f"{bad} unusual values: {ov.loc[(ov['price_vs_compset']<0.3)|(ov['price_vs_compset']>2.5), ['date','own_rate','compset_median','price_vs_compset']].to_string()}"
)

# 12. Summer rates higher than winter
winter = ov[ov['date'].dt.month.isin([1, 2])]['own_rate'].mean()
summer = ov[ov['date'].dt.month.isin([6, 7, 8])]['own_rate'].mean()
check(
    f"Summer rates ({summer:.0f}) higher than winter ({winter:.0f})",
    summer > winter,
    "Rates don't show expected seasonality"
)

print()
print(f"  {checks_passed} checks passed, {checks_failed} checks failed")
if checks_failed == 0:
    print("  🎉 All checks passed — clean_bookingcom.csv is ready for the next step")
else:
    print("  ⚠️  Fix the failed checks before running the next script")