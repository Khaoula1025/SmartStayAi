"""
Script 12 — Prophet Seasonality Charts
========================================
Extracts seasonality components from the trained Prophet model and outputs
clean JSON files ready for the frontend dashboard charts.

Outputs:
  - data/prophet/seasonality_yearly.json   : avg occupancy by month (Jan-Dec)
  - data/prophet/seasonality_weekly.json   : avg occupancy by day of week
  - data/prophet/seasonality_trend.json    : long-term trend over training period
  - data/prophet/forecast_comparison.json  : Prophet vs GBM predictions side by side
  - data/prophet/components_full.csv       : full decomposition table (for EDA)

Usage:
  uv run scripts/12_prophet_seasonality.py
  uv run scripts/12_prophet_seasonality.py --plot   # also save PNG charts
"""

import os, sys, json, argparse
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
MODELS_DIR    = Path(os.getenv('MODELS_DIR',   'data/models'))
PRED_PATH     = Path(os.getenv('PRED_PATH',    'data/prediction/predictions_2026.csv'))
PROPHET_PATH  = Path(os.getenv('PROPHET_PATH', 'data/prediction/prophet_predictions.csv'))
TRAIN_PATH    = Path(os.getenv('TRAIN_PATH',   'data/processed/training_matrix.csv'))
PROPHET_DIR   = Path(os.getenv('PROPHET_DIR',  'data/prophet'))
PROPHET_DIR.mkdir(parents=True, exist_ok=True)

# Day/month labels
DOW_LABELS   = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']


def load_prophet_model():
    """Try loading saved Prophet model, fall back to retraining."""
    prophet_model_path = MODELS_DIR / 'prophet_model.joblib'
    if prophet_model_path.exists():
        import joblib
        print(f"  Loading saved Prophet model from {prophet_model_path}")
        return joblib.load(prophet_model_path)
    return None


def retrain_prophet(tr: pd.DataFrame):
    """Retrain Prophet on training data if no saved model found."""
    try:
        from prophet import Prophet
    except ImportError:
        sys.exit("ERROR: prophet not installed. Run: pip install prophet")

    print("  No saved Prophet model found — retraining...")
    prophet_df = tr[['date', 'occ_rate']].rename(columns={'date': 'ds', 'occ_rate': 'y'})
    prophet_df = prophet_df.dropna(subset=['y'])

    m = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10,
    )
    m.add_country_holidays(country_name='GB')
    m.fit(prophet_df)
    return m


def extract_yearly_seasonality(model) -> list:
    """
    Extract yearly seasonality component.
    Returns avg predicted occupancy effect for each month.
    """
    from prophet import Prophet
    # Generate one full year of daily dates
    future = pd.DataFrame({
        'ds': pd.date_range('2026-01-01', '2026-12-31', freq='D')
    })
    forecast = model.predict(future)

    # Keep only yearly seasonality component
    result = forecast[['ds', 'yearly']].copy()
    result['month'] = result['ds'].dt.month

    monthly = result.groupby('month')['yearly'].mean().reset_index()
    monthly['month_name'] = monthly['month'].apply(lambda m: MONTH_LABELS[m-1])

    # Normalize so values represent the occupancy effect in percentage points
    # Prophet outputs multiplicative seasonality as a factor around 1.0
    # Convert to additive-style for easier display
    base = monthly['yearly'].mean()
    monthly['effect_pp'] = ((monthly['yearly'] - base) * 100).round(2)
    monthly['value'] = monthly['yearly'].round(4)

    return [
        {
            "month":      int(row['month']),
            "month_name": row['month_name'],
            "value":      float(row['value']),
            "effect_pp":  float(row['effect_pp']),
        }
        for _, row in monthly.iterrows()
    ]


def extract_weekly_seasonality(model) -> list:
    """
    Extract weekly seasonality component.
    Returns occupancy effect for each day of week.
    """
    # Use 4 weeks of dates to average out yearly effects
    future = pd.DataFrame({
        'ds': pd.date_range('2026-06-01', periods=28, freq='D')  # stable summer period
    })
    forecast = model.predict(future)
    result = forecast[['ds', 'weekly']].copy()
    result['dow'] = result['ds'].dt.dayofweek  # 0=Mon, 6=Sun

    weekly = result.groupby('dow')['weekly'].mean().reset_index()
    base   = weekly['weekly'].mean()
    weekly['effect_pp'] = ((weekly['weekly'] - base) * 100).round(2)

    return [
        {
            "dow":        int(row['dow']),
            "day_name":   DOW_LABELS[int(row['dow'])],
            "day_short":  DOW_LABELS[int(row['dow'])][:3],
            "value":      round(float(row['weekly']), 4),
            "effect_pp":  float(row['effect_pp']),
        }
        for _, row in weekly.iterrows()
    ]


def extract_trend(model, tr: pd.DataFrame) -> list:
    """
    Extract long-term trend over the training period + 2026 forecast.
    Shows whether the hotel is growing or declining in base demand.
    """
    # Full period: training start to end of 2026
    date_min = tr['date'].min()
    future   = pd.DataFrame({
        'ds': pd.date_range(date_min, '2026-12-31', freq='D')
    })
    forecast = model.predict(future)

    # Weekly resample for cleaner chart
    trend_weekly = forecast[['ds', 'trend']].set_index('ds').resample('W').mean().reset_index()
    trend_weekly['is_forecast'] = trend_weekly['ds'] > tr['date'].max()

    return [
        {
            "date":        row['ds'].strftime('%Y-%m-%d'),
            "trend":       round(float(row['trend']), 4),
            "trend_pct":   round(float(row['trend']) * 100, 1),
            "is_forecast": bool(row['is_forecast']),
        }
        for _, row in trend_weekly.iterrows()
    ]


def build_forecast_comparison(model, tr: pd.DataFrame) -> list:
    """
    Compare Prophet predictions vs GBM ensemble for 2026.
    This is the cross-validation story: when they agree = high confidence.
    """
    # Load GBM predictions
    if not PRED_PATH.exists():
        print("  WARNING: predictions_2026.csv not found, skipping comparison.")
        return []

    gbm_pred = pd.read_csv(PRED_PATH, parse_dates=['date'])
    gbm_pred = gbm_pred[gbm_pred['date'].dt.year == 2026].copy()

    # Get Prophet forecast for 2026
    future_2026 = pd.DataFrame({'ds': pd.date_range('2026-01-01', '2026-12-31', freq='D')})
    prophet_fc  = model.predict(future_2026)
    prophet_fc  = prophet_fc[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    prophet_fc  = prophet_fc.rename(columns={'ds': 'date'})
    prophet_fc['date'] = pd.to_datetime(prophet_fc['date'])

    # Clip to valid occupancy range
    for col in ['yhat', 'yhat_lower', 'yhat_upper']:
        prophet_fc[col] = prophet_fc[col].clip(0, 1)

    # Merge
    merged = gbm_pred[['date', 'predicted_occ', 'occ_low', 'occ_high', 'rate_tier']].merge(
        prophet_fc, on='date', how='inner'
    )

    # Compute agreement: |GBM - Prophet| < 10pp = agree
    merged['agreement_gap_pp'] = ((merged['predicted_occ'] - merged['yhat']).abs() * 100).round(1)
    merged['models_agree']     = merged['agreement_gap_pp'] < 10.0
    merged['confidence']       = merged['models_agree'].apply(
        lambda x: 'high' if x else 'low'
    )

    return [
        {
            "date":             row['date'].strftime('%Y-%m-%d'),
            "gbm_occ":          round(float(row['predicted_occ']), 4),
            "gbm_occ_pct":      round(float(row['predicted_occ']) * 100, 1),
            "gbm_low":          round(float(row['occ_low']), 4),
            "gbm_high":         round(float(row['occ_high']), 4),
            "prophet_occ":      round(float(row['yhat']), 4),
            "prophet_occ_pct":  round(float(row['yhat']) * 100, 1),
            "prophet_low":      round(float(row['yhat_lower']), 4),
            "prophet_high":     round(float(row['yhat_upper']), 4),
            "agreement_gap_pp": float(row['agreement_gap_pp']),
            "models_agree":     bool(row['models_agree']),
            "confidence":       row['confidence'],
            "rate_tier":        str(row['rate_tier']),
        }
        for _, row in merged.iterrows()
    ]


def save_png_charts(yearly, weekly, trend, comparison):
    """Optional: save static PNG charts (for EDA notebook or report)."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        NAVY, GOLD, TEAL, RED = '#1C2B4A', '#C9A84C', '#2a9d8f', '#e76f51'
        plt.rcParams.update({'font.family': 'sans-serif', 'axes.spines.top': False,
                             'axes.spines.right': False})

        fig, axes = plt.subplots(2, 2, figsize=(16, 11))
        fig.suptitle('SmartStay Intelligence — Prophet Seasonality Analysis\nThe Hickstead Hotel',
                     fontsize=14, fontweight='bold', color=NAVY, y=1.01)

        # Yearly seasonality
        ax = axes[0, 0]
        months = [d['month_name'] for d in yearly]
        effects = [d['effect_pp'] for d in yearly]
        colors = [GOLD if e > 0 else TEAL for e in effects]
        ax.bar(months, effects, color=colors, edgecolor='white', alpha=0.85)
        ax.axhline(0, color=NAVY, linewidth=1.5, linestyle='--')
        ax.set_title('Yearly Seasonality Effect (pp)', fontweight='bold', color=NAVY)
        ax.set_ylabel('Occupancy effect (percentage points)')
        ax.tick_params(axis='x', rotation=45)

        # Weekly seasonality
        ax = axes[0, 1]
        days    = [d['day_short'] for d in weekly]
        effects_w = [d['effect_pp'] for d in weekly]
        colors_w  = [GOLD if e > 0 else TEAL for e in effects_w]
        ax.bar(days, effects_w, color=colors_w, edgecolor='white', alpha=0.85)
        ax.axhline(0, color=NAVY, linewidth=1.5, linestyle='--')
        ax.set_title('Weekly Seasonality Effect (pp)', fontweight='bold', color=NAVY)
        ax.set_ylabel('Occupancy effect (percentage points)')

        # Trend
        ax = axes[1, 0]
        trend_dates  = pd.to_datetime([t['date'] for t in trend])
        trend_values = [t['trend_pct'] for t in trend]
        is_forecast  = [t['is_forecast'] for t in trend]
        hist_dates   = [d for d, f in zip(trend_dates, is_forecast) if not f]
        hist_vals    = [v for v, f in zip(trend_values, is_forecast) if not f]
        fc_dates     = [d for d, f in zip(trend_dates, is_forecast) if f]
        fc_vals      = [v for v, f in zip(trend_values, is_forecast) if f]
        ax.plot(hist_dates, hist_vals, color=NAVY, linewidth=2, label='Historical trend')
        if fc_dates:
            ax.plot(fc_dates, fc_vals, color=GOLD, linewidth=2, linestyle='--', label='Forecast trend')
        ax.axvline(pd.Timestamp('2026-01-01'), color=RED, linewidth=1, linestyle=':', alpha=0.7)
        ax.set_title('Long-term Trend', fontweight='bold', color=NAVY)
        ax.set_ylabel('Trend occupancy (%)')
        ax.legend()

        # GBM vs Prophet comparison (monthly avg)
        ax = axes[1, 1]
        if comparison:
            comp_df   = pd.DataFrame(comparison)
            comp_df['date'] = pd.to_datetime(comp_df['date'])
            monthly   = comp_df.set_index('date').resample('ME').agg(
                {'gbm_occ_pct': 'mean', 'prophet_occ_pct': 'mean'}
            ).reset_index()
            ax.plot(monthly['date'], monthly['gbm_occ_pct'],
                    color=NAVY, linewidth=2.5, label='GBM+RF ensemble')
            ax.plot(monthly['date'], monthly['prophet_occ_pct'],
                    color=GOLD, linewidth=2, linestyle='--', label='Prophet')
            agree_pct = sum(d['models_agree'] for d in comparison) / len(comparison) * 100
            ax.set_title(f'GBM vs Prophet 2026 Forecast\n(Models agree on {agree_pct:.0f}% of dates)',
                         fontweight='bold', color=NAVY)
            ax.set_ylabel('Predicted occupancy (%)')
            ax.legend()
        else:
            ax.text(0.5, 0.5, 'No comparison data', ha='center', va='center',
                    transform=ax.transAxes, color='grey')

        plt.tight_layout()
        chart_path = PROPHET_DIR / 'prophet_seasonality_charts.png'
        plt.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Chart saved: {chart_path}")
    except Exception as e:
        print(f"  WARNING: Could not save PNG charts: {e}")


def main(args):
    print("=" * 60)
    print("Script 12 — Prophet Seasonality Charts")
    print("=" * 60)

    # ── Load training data ────────────────────────────────────────────────────
    print("\n[1/5] Loading training matrix...")
    tr = pd.read_csv(TRAIN_PATH, parse_dates=['date'])
    tr = tr.dropna(subset=['occ_rate'])
    print(f"  Training rows: {len(tr)}  ({tr['date'].min().date()} → {tr['date'].max().date()})")

    # ── Load or retrain Prophet ───────────────────────────────────────────────
    print("\n[2/5] Loading Prophet model...")
    model = load_prophet_model()
    if model is None:
        model = retrain_prophet(tr)
        # Save for future use
        try:
            import joblib
            joblib.dump(model, MODELS_DIR / 'prophet_model.joblib')
            print(f"  Saved Prophet model to {MODELS_DIR}/prophet_model.joblib")
        except Exception as e:
            print(f"  WARNING: Could not save Prophet model: {e}")

    # ── Extract components ────────────────────────────────────────────────────
    print("\n[3/5] Extracting seasonality components...")

    print("  → Yearly seasonality...")
    yearly = extract_yearly_seasonality(model)
    print(f"    Peak month:  {max(yearly, key=lambda x: x['effect_pp'])['month_name']} "
          f"(+{max(yearly, key=lambda x: x['effect_pp'])['effect_pp']:.1f}pp)")
    print(f"    Trough month:{min(yearly, key=lambda x: x['effect_pp'])['month_name']} "
          f"({min(yearly, key=lambda x: x['effect_pp'])['effect_pp']:.1f}pp)")

    print("  → Weekly seasonality...")
    weekly = extract_weekly_seasonality(model)
    print(f"    Best day:   {max(weekly, key=lambda x: x['effect_pp'])['day_name']} "
          f"(+{max(weekly, key=lambda x: x['effect_pp'])['effect_pp']:.1f}pp)")
    print(f"    Worst day:  {min(weekly, key=lambda x: x['effect_pp'])['day_name']} "
          f"({min(weekly, key=lambda x: x['effect_pp'])['effect_pp']:.1f}pp)")

    print("  → Long-term trend...")
    trend = extract_trend(model, tr)
    hist_trend = [t for t in trend if not t['is_forecast']]
    fc_trend   = [t for t in trend if t['is_forecast']]
    print(f"    Historical: {hist_trend[0]['trend_pct']:.1f}% → {hist_trend[-1]['trend_pct']:.1f}%")
    if fc_trend:
        print(f"    Forecast:   {fc_trend[0]['trend_pct']:.1f}% → {fc_trend[-1]['trend_pct']:.1f}%")

    print("  → GBM vs Prophet comparison...")
    comparison = build_forecast_comparison(model, tr)
    if comparison:
        agree_pct = sum(d['models_agree'] for d in comparison) / len(comparison) * 100
        avg_gap   = sum(d['agreement_gap_pp'] for d in comparison) / len(comparison)
        print(f"    Models agree on {agree_pct:.0f}% of dates (avg gap: {avg_gap:.1f}pp)")

    # ── Save outputs ──────────────────────────────────────────────────────────
    print("\n[4/5] Saving JSON files...")

    files = {
        'seasonality_yearly.json':   yearly,
        'seasonality_weekly.json':   weekly,
        'seasonality_trend.json':    trend,
        'forecast_comparison.json':  comparison,
    }
    for filename, data in files.items():
        path = PROPHET_DIR / filename
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"  Saved: {path} ({len(data)} entries)")

    # Full components CSV
    prophet_df = tr[['date', 'occ_rate']].rename(columns={'date': 'ds', 'occ_rate': 'y'})
    prophet_df = prophet_df.dropna(subset=['y'])
    full_fc    = model.predict(model.make_future_dataframe(periods=366))
    components_cols = ['ds', 'trend', 'yhat', 'yhat_lower', 'yhat_upper']
    if 'yearly' in full_fc.columns:
        components_cols.append('yearly')
    if 'weekly' in full_fc.columns:
        components_cols.append('weekly')
    if 'holidays' in full_fc.columns:
        components_cols.append('holidays')

    components_df = full_fc[components_cols].copy()
    components_df['ds'] = pd.to_datetime(components_df['ds']).dt.strftime('%Y-%m-%d')
    components_path = PROPHET_DIR / 'components_full.csv'
    components_df.to_csv(components_path, index=False)
    print(f"  Saved: {components_path}")

    # ── Optional PNG charts ───────────────────────────────────────────────────
    if args.plot:
        print("\n[5/5] Saving PNG charts...")
        save_png_charts(yearly, weekly, trend, comparison)
    else:
        print("\n[5/5] Skipping PNG charts (pass --plot to generate)")

    # ── Print summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("PROPHET SEASONALITY SUMMARY")
    print("=" * 60)

    print("\nYearly pattern (monthly effect in pp):")
    for item in yearly:
        bar = "█" * max(1, int(abs(item['effect_pp'])))
        sign = "+" if item['effect_pp'] > 0 else ""
        print(f"  {item['month_name']:<4} {sign}{item['effect_pp']:>5.1f}pp  {bar}")

    print("\nWeekly pattern (day of week effect in pp):")
    for item in weekly:
        bar = "█" * max(1, int(abs(item['effect_pp'])))
        sign = "+" if item['effect_pp'] > 0 else ""
        print(f"  {item['day_short']:<4} {sign}{item['effect_pp']:>5.1f}pp  {bar}")

    print(f"\n✅ Prophet seasonality complete. Files in: {PROPHET_DIR}/")
    print(f"\nNext step: add GET /api/v1/analytics/seasonality endpoint")
    print(f"           to serve these JSON files to the dashboard.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SmartStay Prophet Seasonality')
    parser.add_argument('--plot', action='store_true',
                        help='Also save static PNG charts')
    args = parser.parse_args()
    main(args)
