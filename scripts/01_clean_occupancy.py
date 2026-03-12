import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FILE_2024   = 'data/raw/UNOHICK_Occupancy_20260227_235408.xlsx'
FILE_2025   = 'data/raw/UNOHICK_Occupancy_20260227_235324.xlsx'
OUTPUT      = 'data/processed/clean_occupancy_5.csv'
TOTAL_ROOMS = 52

ROOM_TYPES  = ['DB', 'DB_SB', 'EXEC', 'TB']
DATE_COLS   = [1, 11, 17, 23, 29]
DATE_MONTHS = ['Jan','Feb','Mar','Apr','May','Jun',
               'Jul','Aug','Sep','Oct','Nov','Dec']

# Column offsets relative to each date's base column
OFF_DATE1   = {'avl':0, 'ov':1, 'let':2, 'tot':4, 'occ':6, 'slprs':8}  # NaN spacers
OFF_COMPACT = {'avl':0, 'ov':1, 'let':2, 'tot':3, 'occ':4, 'slprs':5}  # no spacers

print("=" * 62)
print("SmartStay — Script 01: Clean Occupancy Data")
print("=" * 62)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(row, col):
    """Extract a float from a specific column, return NaN on failure."""
    try:
        if col >= len(row):
            return np.nan
        v = str(row.iloc[col]).strip()
        return float(v) if v not in ('nan', '', 'None') else np.nan
    except (ValueError, TypeError):
        return np.nan


def is_noise_row(label):
    """Page break and hotel title rows — discard."""
    return label.startswith('Page') or label.startswith('The Hickstead')


def row_has_dates(raw, row_idx):
    """Return True if this row contains at least one recognisable date string."""
    for c in DATE_COLS:
        if c >= raw.shape[1]:
            continue
        v = str(raw.iloc[row_idx, c]).strip()
        if any(m in v for m in DATE_MONTHS):
            return True
    return False


def row_is_numeric_data(row):
    """
    Return True if this row's col 1/2/3 contains a number.
    Used to distinguish data rows from column-header rows (Avl/Ov/Let…).
    """
    for c in [1, 2, 3]:
        if c >= len(row):
            continue
        v = str(row.iloc[c]).strip()
        if v not in ('nan', '', 'None'):
            try:
                float(v)
                return True
            except ValueError:
                pass
    return False


def find_next_data_row(raw, start, end):
    """
    Lookahead: find the first non-header numeric nan-label row in [start, end).
    Used for post-label data rows where the Totals label appears before its data.
    """
    for j in range(start, end):
        lbl = str(raw.iloc[j, 0]).strip()
        if is_noise_row(lbl):
            continue
        if lbl in ('nan', '') and row_is_numeric_data(raw.iloc[j]):
            return raw.iloc[j]
        # If we hit a real label (room type / new block / Totals) stop looking
        if lbl not in ('nan', ''):
            break
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CORE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_occupancy_file(filepath):
    """
    Parse one PMS XLSX file into a per-day DataFrame with full room type detail.

    Strategy: queue-based block scanner.
    Maintains a FIFO buffer (data_queue) of unassigned numeric nan-rows.
    When a labelled row has no inline data, it pops from the queue (pre-label data)
    or uses lookahead (post-label data).
    """
    xl  = pd.ExcelFile(filepath)
    raw = xl.parse('Sheet1', header=None, dtype=str)

    records = []

    for i in range(len(raw)):
        # ── Only process Room Type header rows
        if str(raw.iloc[i, 0]).strip() != 'Room Type':
            continue
        # Skip annual totals block ("Room Type | Totals")
        if str(raw.iloc[i, 1]).strip() == 'Totals':
            continue
        # Skip dateless continuation headers produced by page breaks
        if not row_has_dates(raw, i):
            continue

        # ── Collect (base_col, date) pairs from this header row
        date_positions = []
        for c in DATE_COLS:
            if c >= raw.shape[1]:
                continue
            v = str(raw.iloc[i, c]).strip()
            if any(m in v for m in DATE_MONTHS):
                try:
                    date_positions.append((c, pd.to_datetime(v, format='%a %d %b %Y')))
                except ValueError:
                    pass

        if not date_positions:
            continue

        # ── Queue-based forward scan (up to 25 rows, max gap = 13 observed)
        block      = {}   # label -> pandas row that holds its data
        data_queue = []   # buffered pre-label numeric nan-rows

        j = i + 1
        while j < min(i + 25, len(raw)):
            lbl = str(raw.iloc[j, 0]).strip()

            # Skip page-break / title noise
            if is_noise_row(lbl):
                j += 1
                continue

            # Skip dateless Room Type continuation markers
            if lbl == 'Room Type' and not row_has_dates(raw, j):
                j += 1
                continue

            # A new dated block starts → stop scanning
            if lbl == 'Room Type' and row_has_dates(raw, j):
                break

            row_j    = raw.iloc[j]
            has_data = str(row_j.iloc[1]).strip() not in ('nan', '', 'None')
            is_nan   = lbl in ('nan', '')

            if is_nan:
                # Column-header rows (Avl/Ov/Let…) are NOT data rows
                is_header = str(row_j.iloc[1]).strip() == 'Avl'
                if not is_header and row_is_numeric_data(row_j):
                    data_queue.append(row_j)   # buffer for next label

            elif lbl in ROOM_TYPES or lbl == 'Totals':
                if has_data:
                    # Pattern A: label + inline data on same row
                    block[lbl]  = row_j
                    data_queue  = []   # clear buffer — this label was self-contained
                elif data_queue:
                    # Pattern B: data arrived before label (pre-queued)
                    block[lbl]  = data_queue.pop(0)
                else:
                    # Pattern C: label with no data AND no buffer
                    # → look ahead for data on a following nan row
                    lookahead = find_next_data_row(raw, j + 1, min(j + 4, len(raw)))
                    if lookahead is not None:
                        block[lbl] = lookahead

            j += 1

        # ── Can't extract data without Totals row
        if 'Totals' not in block:
            continue

        # ── One record per date in this block
        for k, (base_col, date) in enumerate(date_positions):
            off    = OFF_DATE1 if k == 0 else OFF_COMPACT
            totals = block['Totals']

            rec = {
                'date':      date,
                'avl':       safe_float(totals, base_col + off['avl']),
                'ov':        safe_float(totals, base_col + off['ov']),
                'rooms_let': safe_float(totals, base_col + off['let']),
                'tot_rooms': safe_float(totals, base_col + off['tot']),
                'occ_pct':   safe_float(totals, base_col + off['occ']),
                'sleepers':  safe_float(totals, base_col + off['slprs']),
            }

            # Room type breakdown
            for rt in ROOM_TYPES:
                if rt in block:
                    rt_row = block[rt]
                    rec[f'{rt}_avl']   = safe_float(rt_row, base_col + off['avl'])
                    rec[f'{rt}_ov']    = safe_float(rt_row, base_col + off['ov'])
                    rec[f'{rt}_let']   = safe_float(rt_row, base_col + off['let'])
                    rec[f'{rt}_tot']   = safe_float(rt_row, base_col + off['tot'])
                    rec[f'{rt}_occ']   = safe_float(rt_row, base_col + off['occ'])
                    rec[f'{rt}_slprs'] = safe_float(rt_row, base_col + off['slprs'])
                else:
                    for s in ['avl', 'ov', 'let', 'tot', 'occ', 'slprs']:
                        rec[f'{rt}_{s}'] = np.nan

            records.append(rec)

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'].dt.year.isin([2024, 2025])]
    df = df.sort_values('date').drop_duplicates('date').reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PARSE BOTH FILES
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/8] Parsing 2024 file...")
df_2024 = parse_occupancy_file(FILE_2024)
print(f"      {len(df_2024)} rows | {df_2024.date.min().date()} → {df_2024.date.max().date()}")
print(f"      occ_pct missing from raw: {df_2024['occ_pct'].isna().sum()} rows")

print("\n[2/8] Parsing 2025 file...")
df_2025 = parse_occupancy_file(FILE_2025)
print(f"      {len(df_2025)} rows | {df_2025.date.min().date()} → {df_2025.date.max().date()}")
print(f"      occ_pct missing from raw: {df_2025['occ_pct'].isna().sum()} rows")


# ─────────────────────────────────────────────────────────────────────────────
# REMOVE CLOSED PERIOD  (Jan–Mar 2024)
# Hotel was not operational — all-zero rows would corrupt model training.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/8] Removing Jan/Feb/Mar 2024 (hotel closed — all zeros)...")
before = len(df_2024)
df_2024 = df_2024[
    ~((df_2024['date'].dt.year == 2024) & (df_2024['date'].dt.month <= 3))
].reset_index(drop=True)
print(f"      Removed {before - len(df_2024)} rows → {len(df_2024)} remaining")


# ─────────────────────────────────────────────────────────────────────────────
# COMBINE
# ─────────────────────────────────────────────────────────────────────────────

print("\n[4/8] Combining 2024 + 2025...")
df = pd.concat([df_2024, df_2025]).sort_values('date').drop_duplicates('date').reset_index(drop=True)
print(f"      {len(df)} rows | {df.date.min().date()} → {df.date.max().date()}")


# ─────────────────────────────────────────────────────────────────────────────
# CONVERT OCC_PCT → OCC_RATE
# PMS stores occupancy as 0–100. ML model uses 0–1.
# One 102% entry exists (data error) → capped.
# Same conversion applied to each room type's occ column.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[5/8] Converting occ_pct → occ_rate (0–1)...")
df['occ_rate'] = pd.to_numeric(df['occ_pct'], errors='coerce') / 100
over_100 = (df['occ_rate'] > 1.0).sum()
df['occ_rate'] = df['occ_rate'].clip(0, 1.0)
for rt in ROOM_TYPES:
    col = f'{rt}_occ'
    if col in df.columns:
        df[f'{rt}_occ_rate'] = (pd.to_numeric(df[col], errors='coerce') / 100).clip(0, 1.0)
print(f"      {over_100} rows capped from >100% to 1.0")


# ─────────────────────────────────────────────────────────────────────────────
# CALENDAR FEATURES
# ─────────────────────────────────────────────────────────────────────────────

print("\n[6/8] Adding calendar features...")
df['year']           = df['date'].dt.year
df['month']          = df['date'].dt.month
df['dow']            = df['date'].dt.day_name().str[:3]
df['day_of_week']    = df['date'].dt.dayofweek
df['week_of_year']   = df['date'].dt.isocalendar().week.astype(int)
df['is_weekend']     = df['date'].dt.dayofweek.isin([4, 5, 6]).astype(int)
df['is_high_season'] = df['month'].isin([4, 5, 6, 7, 9, 10, 11, 12]).astype(int)
print("      year, month, dow, day_of_week, week_of_year, is_weekend, is_high_season")


# ─────────────────────────────────────────────────────────────────────────────
# GAP FILLING  (time-based linear interpolation)
#
# 4 genuine PMS gaps remain in Dec 2025 (raw block exists, no Totals data).
# Strategy: interpolate between surrounding real values — smooth and appropriate
# for occupancy which changes gradually day-to-day.
# Applied to: all numeric columns.
# is_interpolated flag set BEFORE filling to track synthetic rows accurately.
# ─────────────────────────────────────────────────────────────────────────────

print("\n[7/8] Filling remaining gaps via time interpolation...")

raw_missing      = df['occ_rate'].isna()
missing_before   = int(raw_missing.sum())
print(f"      Raw missing rows: {missing_before}")

if missing_before > 0:
    gap_by_month = df[raw_missing].groupby(df['date'].dt.to_period('M')).size()
    for period, count in gap_by_month.items():
        total = int((df['date'].dt.to_period('M') == period).sum())
        print(f"        {period}: {count}/{total} days missing")

# Build list of numeric columns to interpolate
interp_cols = ['occ_rate', 'rooms_let', 'sleepers', 'tot_rooms', 'avl', 'ov']
for rt in ROOM_TYPES:
    for suffix in ['avl', 'ov', 'let', 'tot', 'occ_rate', 'slprs']:
        col = f'{rt}_occ_rate' if suffix == 'occ_rate' else f'{rt}_{suffix}'
        if col in df.columns:
            interp_cols.append(col)

df = df.sort_values('date').set_index('date')

for col in interp_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].interpolate(method='time', limit=30, limit_direction='both')

df = df.reset_index()

# Clamp after interpolation
df['occ_rate']  = df['occ_rate'].clip(0, 1.0).round(6)
df['rooms_let'] = df['rooms_let'].clip(0).round(1)
df['sleepers']  = df['sleepers'].clip(0).round(1)
df['tot_rooms'] = df['tot_rooms'].fillna(TOTAL_ROOMS).clip(0, TOTAL_ROOMS)
df['avl']       = df['avl'].clip(0).round(1)

# Recalculate occ_pct from interpolated occ_rate (keeps columns in sync)
df['occ_pct'] = (df['occ_rate'] * 100).round(1)

# Clamp room-type occ_rates
for rt in ROOM_TYPES:
    col = f'{rt}_occ_rate'
    if col in df.columns:
        df[col] = df[col].clip(0, 1.0).round(6)

# is_interpolated: 1 for every row that had no raw occ_rate value
df['is_interpolated'] = raw_missing.astype(int).values

missing_after = int(df['occ_rate'].isna().sum())
print(f"      Filled {missing_before - missing_after} rows | Still missing: {missing_after}")


# ─────────────────────────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────────────────────────

print("\n[8/8] Saving...")
df.to_csv(OUTPUT, index=False)

print("\n" + "=" * 62)
print("✅  DONE")
print("=" * 62)
print(f"  Output:  {OUTPUT}")
print(f"  Rows:    {len(df)}")
print(f"  Columns: {len(df.columns)}  ({list(df.columns)})")
print()
print(f"  Interpolated rows: {df['is_interpolated'].sum()} / {len(df)}")
print()

# Monthly summary with room type averages
print("  Monthly occupancy (* = interpolated days present):")
for yr in [2024, 2025]:
    yr_df = df[df['year'] == yr]
    for m in range(1, 13):
        g = yr_df[yr_df['month'] == m]
        if len(g) == 0:
            continue
        avg_occ  = g['occ_rate'].mean()
        bar      = '█' * int(avg_occ * 25)
        interp   = ' *' if g['is_interpolated'].sum() > 0 else ''
        rt_parts = []
        for rt in ROOM_TYPES:
            col = f'{rt}_occ_rate'
            if col in g.columns:
                rt_parts.append(f"{rt}:{g[col].mean():.0%}")
        rt_str = '  [' + '  '.join(rt_parts) + ']'
        print(f"    {yr}-{m:02d}  {bar:<25} {avg_occ:.1%}{interp}{rt_str}")


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION  (20 checks)
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 62)
print("🔍  VALIDATION")
print("=" * 62)

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

# ── Row counts
check("2025 has 364 rows",
      (df['date'].dt.year == 2025).sum() == 364,
      f"Got {(df['date'].dt.year==2025).sum()}")
check("2024 (Apr–Dec) has 274 rows",
      (df['date'].dt.year == 2024).sum() == 274,
      f"Got {(df['date'].dt.year==2024).sum()}")
check("Total rows = 638", len(df) == 638, f"Got {len(df)}")

# ── Date integrity
check("No duplicate dates",
      df['date'].duplicated().sum() == 0,
      f"{df['date'].duplicated().sum()} dupes")
check("Dates sorted ascending", df['date'].is_monotonic_increasing)
check("No Jan/Feb/Mar 2024 rows",
      ((df['date'].dt.year == 2024) & (df['date'].dt.month <= 3)).sum() == 0)

# ── Core occupancy — zero missing after interpolation
check("occ_rate — zero missing", df['occ_rate'].isna().sum() == 0,
      f"{df['occ_rate'].isna().sum()} still missing")
check("occ_rate all 0.0–1.0",
      ((df['occ_rate'] >= 0) & (df['occ_rate'] <= 1)).all())
check("rooms_let — zero missing", df['rooms_let'].isna().sum() == 0)
check("tot_rooms — zero missing", df['tot_rooms'].isna().sum() == 0)
check("tot_rooms never exceeds 52",
      (df['tot_rooms'] <= TOTAL_ROOMS).all(),
      f"Max: {df['tot_rooms'].max()}")
check("occ_pct in sync with occ_rate",
      ((df['occ_pct'] - df['occ_rate'] * 100).abs() < 0.2).all())

# ── Room type columns — all present and zero missing
for rt in ROOM_TYPES:
    col = f'{rt}_let'
    miss = df[col].isna().sum() if col in df.columns else len(df)
    check(f"{rt}_let present, zero missing",
          col in df.columns and miss == 0,
          f"{miss} missing" if col in df.columns else "column absent")

# ── Seasonality
jan = df[(df['year']==2025) & (df['month']==1)]['occ_rate'].mean()
jul = df[(df['year']==2025) & (df['month']==7)]['occ_rate'].mean()
summer = df[(df['year']==2025) & (df['month'].isin([6,7,8]))]['occ_rate'].mean()
check(f"Seasonality: Jan 2025 ({jan:.1%}) < Jul 2025 ({jul:.1%})", jan < jul)
check(f"Summer 2025 avg > 70% (got {summer:.1%})", summer > 0.70)

# ── Calendar columns
check("day_of_week 0–6",       df['day_of_week'].between(0,6).all())
check("is_weekend 0 or 1",     df['is_weekend'].isin([0,1]).all())
check("is_high_season 0 or 1", df['is_high_season'].isin([0,1]).all())
check("is_interpolated 0 or 1",df['is_interpolated'].isin([0,1]).all())

print()
print(f"  {passed} checks passed, {failed} checks failed")
if failed == 0:
    print("  🎉 All checks passed — clean_occupancy.csv is ready")
else:
    print("  ⚠️  Fix failures before proceeding")
